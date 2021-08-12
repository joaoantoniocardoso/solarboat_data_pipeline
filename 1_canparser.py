#!/usr/bin/env python3
# coding: utf-8

import sys
import os
from timeit import default_timer as timer
import multiprocessing
import re
import json
from typing import Optional, Callable, List
import numpy as np
import pandas as pd
import vaex

from canparser_generator import CanTopicParser


class CanIds:
    """
    Usage:
        schema = load_can_ids('can_ids.json')

        # Print each topic of each module:
        [print(module, topic)
         for topic in schema['modules'][module]['topics']
         for module in schema['modules']]

        # Access a specific topic of a specific module:
        parsed_id, parsed_signature = 33, 250
        module = schema['modules'].get(parsed_signature)
        topic = module['topics'].get(parsed_id)
        print(module['name'], topic['name'])
    """

    @staticmethod
    def load(filename: str) -> dict:
        with open(filename) as schema_file:
            schema = json.load(schema_file)

            modules = {}
            for module in schema["modules"]:
                topics = {}
                for topic in module["topics"]:
                    topics[topic["id"]] = topic
                modules[module["signature"]] = module
                module["topics"] = topics
            schema["modules"] = modules

            return schema


class Datasets:
    def __init__(self, datasets: list, input_path: str, output_path: str = None):
        if output_path is None:
            output_path = input_path

        self.datasets = datasets

        for d in self.datasets:
            if "from" in d and "to" in d:
                d["offset"] = d["to"] - d["from"]
            else:
                d["offset"] = pd.Timedelta("0")
            d["input_path"] = input_path
            d["output_path"] = output_path

    def as_list(self):
        return self.datasets


def parse_payload(
    topic: dict, payload: bytearray, verbose: bool = False, warning: bool = True
) -> Optional[List[dict]]:
    if verbose:
        print(payload, len(payload))

    topic_parser = topic["parser"]
    expected_payload_length = topic_parser.size
    payload_length = len(payload)
    if payload_length != expected_payload_length:
        if warning:
            print(
                f"Warning: wrong payload size. "
                f"Expected: {expected_payload_length}, "
                f"got: {payload_length} {payload}."
            )
        return None

    parsed_payload = topic_parser.from_buffer(bytearray(payload)).as_dict()

    if verbose:
        print(parsed_payload)

    payload_data_list = []
    for b, parsed_byte_name in enumerate(parsed_payload):
        parsed_byte_value = parsed_payload[parsed_byte_name]
        parsed_byte_units = topic["bytes"][b]["units"]

        parsed_byte_units, parsed_byte_value = CanTopicParser.apply_units(
            parsed_byte_units, parsed_byte_value
        )

        parsed_dict = {
            "byte_name": parsed_byte_name,
            "value": parsed_byte_value,
            "unit": parsed_byte_units,
        }
        payload_data_list += [parsed_dict]

        if verbose:
            print(parsed_dict)

    return payload_data_list


def process_message(parsed: list, verbose: bool = False) -> Optional[list]:
    parsed["signature"] = parsed["payload"][0]

    # Fixing BUGS related to wrong configs in some can modules
    if parsed["topic"] == 65:
        parsed["signature"] = 230
        # see https://github.com/ZeniteSolar/MAB20/issues/6
        parsed["payload"] = parsed["payload"][:2]
    elif parsed["topic"] == 64:
        parsed["signature"] = 230

    module = schema["modules"].get(parsed["signature"], None)
    if module is None:
        if verbose:
            print("module =", module, "parsed =", parsed, parsed["payload"])
        return None

    topic = module["topics"].get(parsed["topic"], None)
    if topic is None:
        if verbose:
            print("topic =", topic, "parsed =", parsed, parsed["payload"])
        return None

    parsed_data_dict_list = parse_payload(topic, parsed["payload"])
    if parsed_data_dict_list is None:
        if verbose:
            print(
                "parsed_data_dict_list =",
                parsed_data_dict_list,
                "parsed =",
                parsed,
                parsed["payload"],
            )
        return None

    parsed_data_dict_list = [
        dict(
            item,
            **{
                "timestamp": parsed["timestamp"],
                "module_name": module["name"],
                "topic_name": topic["name"],
            },
        )
        for item in parsed_data_dict_list
    ]

    return parsed_data_dict_list


def apply_and_expand(
    df: pd.DataFrame, f: Callable[[pd.DataFrame], pd.DataFrame]
) -> pd.DataFrame:
    """Apply a function 'f' that returns multiple rows to 'df'.
    A reindexed DataFrame will be returned with all new rows."""

    processed_messages = []
    for message in df.to_dict("records"):
        processed_message = f(message)
        if not processed_message:
            continue
        processed_messages += processed_message

    return pd.DataFrame.from_dict(processed_messages)


def process_chunk(
    s: pd.Series,
    p: re.Pattern,
    flags,
    dataset_info: dict,
) -> pd.DataFrame:

    # Apply ReGex
    df = s.str.extractall(p, flags=flags)

    print(
        f'{dataset_info["input_filename"]}. Length before extract: {len(s)}.\tLength after extract: {len(df)}.'
    )

    # Interpret and fix timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s") + dataset_info["offset"]

    # The first and the last timestamps are always correct,
    # but there is some intermediate that is wrong, so we remove them
    start_timestamp = df["timestamp"].values[0]
    end_timestamp = df["timestamp"].values[-1]
    mask = (df["timestamp"] >= start_timestamp) & (df["timestamp"] <= end_timestamp)
    df = df.loc[mask]
    print(f"Length after timestamp crop: {len(s)}.")

    # The topic_id should be interpreted as a hex number
    df["topic"] = df["topic"].apply(lambda x: int(x, 16)).astype("category")

    # The payload should be interpreted as an array of hex numbers
    df["payload"] = df["payload"].apply(lambda x: bytes.fromhex(x))

    # Processing pipeline
    groups = ["unit", "byte_name", "topic_name", "module_name"]
    df = (
        apply_and_expand(df=df, f=process_message)
        # Same as pivot(), but a little faster:
        .groupby(
            by=[*groups, "timestamp"],
            sort=False,
        )
        .mean()
        .unstack(groups[::-1])
        .rename_axis("timestamp")
        # Downcast
        .astype("float16")
    )

    # Rename columns, going from multi-index to simple index
    separator = "__"
    df.columns = [separator.join(c[1:-1]) for c in df.columns]

    return df


def clean_timestamp_outliers(
    df: vaex.dataframe.DataFrameLocal,
) -> vaex.dataframe.DataFrameLocal:
    s = int(1e4)
    timestamp_diff = np.hstack(
        [
            np.zeros(s, dtype="timedelta64"),
            df["timestamp"].values[s:] - df["timestamp"].values[:-s],
        ]
    ).astype("float64")

    th = 1e11
    df["outlier"] = ((timestamp_diff < -th) & (timestamp_diff > -10 * th)) | (
        (timestamp_diff > th) & (timestamp_diff < 10 * th)
    )

    return df[df["outlier"] == False].drop(["outlier"])


def process_candump_file(
    dataset_info: dict, chunksize: int, output_file_format: str = ".hdf5", verbose=False
) -> dict:
    time_start = timer()

    # REGEX
    # input example: '(1580415599.609366) can0 011#E4360F0000780216'
    regex_pattern = "".join(
        [
            r"\((?P<timestamp>\d{10}\.\d{6})\)\s",
            r"(?P<interface>\w+)\s",
            r"(?P<topic>[0-9a-f]{3})\#",
            r"(?P<payload>(?:[0-9a-f]{2}){2,8})",
            r"(?!\w)",
        ]
    )
    regex_flags = re.IGNORECASE | re.ASCII

    input_filename = dataset_info["input_filename"]
    input_file = dataset_info["input_path"] + "/" + input_filename
    reader = pd.read_csv(
        input_file,
        names=["log_data"],
        chunksize=chunksize,
        encoding="ISO-8859-1",
        lineterminator="\n",
        engine="c",
        low_memory=False,
        dtype=str,
        memory_map=True,
        skip_blank_lines=True,
        on_bad_lines="skip",
    )

    total_input_lines = 0
    total_output_lines = 0
    total_time_elapsed = timer() - time_start

    for c_index, chunk in enumerate(reader):
        chunk_time_start = timer()

        output_filename = (
            input_filename + "_chunk_" + "{:03d}".format(c_index) + output_file_format
        )
        output_file = dataset_info["output_path"] + "/" + output_filename
        if verbose:
            print("output file:    ", output_file)
        if os.path.isfile(output_file):
            if verbose:
                print("\t -> already converted, skipping this chunk...")
            continue

        df = process_chunk(
            chunk["log_data"],
            regex_pattern,
            regex_flags,
            dataset_info,
        )

        if verbose:
            print(df.head(1).append(df.tail(1)))
        if verbose:
            print(df.info(verbose=True, memory_usage="deep"))

        df = vaex.from_pandas(df.reset_index())
        # Clean timestamp outliers
        if "db" not in input_filename:
            df = clean_timestamp_outliers(df)

        # Save the processed chunk to file
        df.export(output_file)

        chunk_time_end = timer()
        chunk_time_elapsed = chunk_time_end - chunk_time_start
        chunk_input_lines = len(chunk)
        chunk_output_lines = len(df)
        if verbose:
            print(
                *[
                    f"Chunk {c_index},",
                    f"elapsed: {chunk_time_elapsed} s,",
                    f"output/input: {chunk_output_lines}/{chunk_input_lines} lines.",
                ]
            )

        total_input_lines += chunk_input_lines
        total_output_lines += chunk_output_lines
        total_time_elapsed += chunk_time_elapsed

    return {
        "Input File Name": input_filename,
        "Output File Name": output_filename,
        "Elapsed time": total_time_elapsed,
        "Input lines": total_input_lines,
        "Output lines": total_output_lines,
    }


def dataset_processor(
    dataset_info: dict,
    chunksize: int,
):

    print("Processing file:", dataset_info["input_filename"])

    report = process_candump_file(dataset_info, chunksize)

    report_str = [
        "-" * 80 + "\n",
        f"Finished Input File: {dataset_info['input_filename']}\n",
        f"\tElapsed time: {report['Elapsed time']} s\n",
    ]
    if report["Input lines"] > 0:
        lines_per_ms = report["Elapsed time"] * 1000 / report["Input lines"]
        report_str += [
            f"\tConversion rate: {lines_per_ms} ms per line\n",
            f"\toutput/input: {report['Output lines']}/{report['Input lines']} lines.\n"
            f"\tSaved to: {report['Output File Name']}\n",
        ]
    report_str += ["=" * 80 + "\n"]

    print(*report_str)


def process_dataset(
    dataset_info_list: List[dict], chunksize: int, parallel: bool = True
):

    returns = []
    p = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    for dataset_info in dataset_info_list:
        if parallel:
            returns += [
                p.apply_async(dataset_processor, args=(dataset_info, chunksize))
            ]
        else:
            dataset_processor(dataset_info, chunksize)
    for x in returns:
        x.get()

    p.close()
    p.join()


def main2020():
    global schema

    schema = CanIds.load("can_ids_old.json")
    schema = CanTopicParser.generate_parsers(schema)

    datasets = [
        # { 'input_filename': 'candump-2020-01-29_111700.log', },
        # { 'input_filename': 'candump-2020-01-29_114446.log', },
        # { 'input_filename': 'candump-2020-01-29_154348.log', },
        # { 'input_filename': 'candump-2020-01-30_054740.log', },
        # { 'input_filename': 'candump-2020-01-30_171953.log', },
        # { 'input_filename': 'candump-2020-01-30_171958.log', },
        # { 'input_filename': 'candump-2020-01-30_171959.log', },
        # { 'input_filename': 'candump-2020-02-01_002021.log', },
        # { 'input_filename': 'candump-2020-02-01_064221.log', },
        {
            "input_filename": "candump-2020-01-29_115602.log",
            "description": "Prova 1, Curta do dia 2020-01-29 13:51:59-03:00",
            "from": pd.Timestamp("2020-01-29 16:51:08.332"),
            "to": pd.Timestamp("2020-01-29 13:51:59"),
        },
        {
            "input_filename": "candump-2020-01-30_054738.log",
            "description": "Prova 2, Longa do dia 2020-01-30 11:16:45-03:00, dados incompletos (deveria ter 03:38:45)",
            "from": pd.Timestamp("2020-01-30 10:02:30.768")
            + pd.Timedelta("0 days 00:00:00.003666"),
            "to": pd.Timestamp("2020-01-30 11:16:45"),
        },
        {
            "input_filename": "candump-2020-01-30_172000.log",
            "description": "Prova 3, Revezamento do dia 2020-01-31 11:23:23",
            "from": pd.Timestamp("2020-01-30 23:32:30.720")
            + pd.Timedelta("0 days 00:00:33.279758")
            + pd.Timedelta("0 days 00:00:00.392470")
            + pd.Timedelta("0 days 00:00:00.006595"),
            "to": pd.Timestamp("2020-01-31 13:50:06.009"),
        },
        {
            "input_filename": "candump-2020-02-01_064223.log",
            "description": "Prova 5, Curta do dia 2020-02-01 13:15:09-03:00",
            "from": pd.Timestamp("2020-02-01 09:51:06.384")
            + pd.Timedelta("0 days 00:00:00.565093")
            - pd.Timedelta("0 days 00:00:01.053649")
            - pd.Timedelta("0 days 00:00:00.013652"),
            "to": pd.Timestamp("2020-02-01 13:15:57.592"),
        },
        {
            "input_filename": "candump-2020-02-01_064222.log",
            "description": "Prova 6, Slalom, e 7, Sprint",
            "from": pd.Timestamp("2020-02-01 11:46:58.964")
            + pd.Timedelta("0 days 00:00:40.016623")
            + pd.Timedelta("0 days 00:00:00.296865")
            + pd.Timedelta("0 days 00:00:00.105090"),
            "to": pd.Timestamp("2020-02-02 10:05:41.987"),
        },
        {
            "input_filename": "candump-from_db0.log",
            "description": "Provas 2 e 3. Dados do TCC do Vinicius Cardoso",
        },
        {
            "input_filename": "candump-from_db1.log",
            "description": "Provas 4, 5 e 6. Dados do TCC do Vinicius Cardoso",
        },
    ]

    dataset_info_list = Datasets(
        datasets=datasets,
        input_path="../data/candump",
        output_path="../data/parsed/sparse",
    ).as_list()

    chunksize = 1_000_000
    process_dataset(
        dataset_info_list,
        chunksize=chunksize,
        parallel=True,
    )


def main2022():
    global schema

    schema = CanIds.load("can_ids_old.json")
    schema = CanTopicParser.generate_parsers(schema)

    datasets = [
        {
            "input_filename": "candump-2022-03-15_205017.log",
            "description": "Provas 1 e 2 - Match Race - 2022-03-17",
        },
        {
            "input_filename": "candump-2022-03-18_011750.log",
            "description": "Provas 3 - Prova Longa - 2022-03-18",
        },
    ]

    dataset_info_list = Datasets(
        datasets=datasets,
        input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/candump",
        output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed/sparse",
    ).as_list()

    chunksize = 1_000_000
    process_dataset(
        dataset_info_list,
        chunksize=chunksize,
        parallel=True,
    )


def main2022_ita():
    global schema

    schema = CanIds.load("can_ids.json")
    schema = CanTopicParser.generate_parsers(schema)

    datasets = [
        # {
        #     "input_filename": "candump-2022-10-14_080650.log",
        #     "description": "Provas 1 e 2 - Match Race - 2022-10-12",
        # },
        # {
        #     "input_filename": "candump-2022-10-13_?.log",
        #     "description": "Interestadual - 2022-10-13",
        # },
        # {
        #     "input_filename": "candump-2022-10-14_080650.log",
        #     "description": "Prova Longa - 2022-10-14",
        # },
        # {
        #     # "input_filename": "candump-2022-10-15_101750.log",
        #     # "input_filename": "candump-2022-10-15_101751.log",
        #     "input_filename": "candump-2022-10-15_111751.log",
        #     "description": "Prova Curta - 2022-10-15",
        # },
        {
            # "input_filename": "candump-2022-10-15_195042.log",
            # "input_filename": "candump-2022-10-15_223341.log",  # placas essenciais
            # "input_filename": "candump-2022-10-15_161750.log",
            # "input_filename": "candump-2022-10-15_230913.log",
            "input_filename": "candump-2022-10-15_234814.log",
            "description": "Debugging - incrementtos de duty-cycle - 2022-10-15",
        },
    ]

    dataset_info_list = Datasets(
        datasets=datasets,
        input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/candump",
        output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed/sparse",
    ).as_list()

    chunksize = 1_000_000
    process_dataset(
        dataset_info_list,
        chunksize=chunksize,
        parallel=True,
    )


if __name__ == "__main__":
    # main2020()
    # main2022()
    main2022_ita()
