import os
from timeit import default_timer as timer
import multiprocessing
import re
import json
from typing import Optional, Callable, List
import numpy as np
import pandas as pd
import vaex

from lib.canparser_generator import CanTopicParser


# Why this is global? -> multiprocessing.Pool.apply_async needs all parameters to be pickleable.
# `schema` uses `ctypes`, which are not easy to make pickleable, so the easiest way is it to be global.
schema: dict


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
    def __init__(
        self, datasets: list, input_path: str, output_path: Optional[str] = None
    ):
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


def process_message(
    parsed: dict, verbose: bool = False, mab20_workaround: bool = False
) -> Optional[list]:
    parsed["signature"] = parsed["payload"][0]

    global schema

    # Fixing BUGS related to wrong configs in some can modules
    if mab20_workaround:
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
    df: pd.DataFrame, f: Callable[[pd.DataFrame], pd.DataFrame], **kwargs
) -> pd.DataFrame:
    """Apply a function 'f' that returns multiple rows to 'df'.
    A reindexed DataFrame will be returned with all new rows."""

    processed_messages = []
    for message in df.to_dict("records"):
        processed_message = f(message, **kwargs)  # type: ignore
        if not processed_message:
            continue
        processed_messages += processed_message

    return pd.DataFrame.from_dict(processed_messages)  # type: ignore


def process_chunk(
    s: pd.Series, p: str, flags, dataset_info: dict, mab20_workaround: bool = False
) -> pd.DataFrame:
    # Apply ReGex
    df: pd.DataFrame = s.str.extractall(p, flags=flags)

    print(
        f'{dataset_info["input_filename"]}. Length before extract: {len(s)}.\tLength after extract: {len(df)}.'
    )

    # Interpret and fix timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    print(
        f'Applying offset of {dataset_info["offset"]}. Going from {df["timestamp"].iloc[0]} to {df["timestamp"].iloc[0] + dataset_info["offset"]}'
    )
    df["timestamp"] += dataset_info["offset"]

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
        apply_and_expand(df=df, f=process_message, mab20_workaround=mab20_workaround)  # type: ignore
        # Same as pivot(), but a little faster:
        .groupby(
            by=[*groups, "timestamp"],
            sort=False,
        )
        .mean()
        .unstack(groups[::-1])  # type: ignore
        .rename_axis("timestamp")
        # Downcast
        .astype(np.float16)
    )

    # Rename columns, going from multi-index to simple index
    separator = "__"
    df.columns = [separator.join(c[1:-1]) for c in df.columns]  # type: ignore

    return df


def clean_timestamp_outliers(
    df: vaex.dataframe.DataFrameLocal,  # type: ignore
) -> vaex.dataframe.DataFrameLocal:  # type: ignore
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
    dataset_info: dict,
    chunksize: int,
    output_file_format: str = ".hdf5",
    verbose=False,
    mab20_workaround: bool = False,
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
    output_filename = ""

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
            mab20_workaround,
        )

        if verbose:
            print(df.head(1).append(df.tail(1)))  # type: ignore
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
        chunk_output_lines = len(df)  # type: ignore
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
    mab20_workaround: bool = False,
) -> None:
    print("Processing file:", dataset_info["input_filename"])

    report = process_candump_file(
        dataset_info, chunksize, mab20_workaround=mab20_workaround
    )

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
    dataset_info_list: List[dict],
    _schema: dict,
    chunksize: int,
    parallel: bool,
    mab20_workaround: bool = False,
) -> None:
    global schema
    schema = _schema

    returns = []
    p = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    for dataset_info in dataset_info_list:
        if parallel:
            returns += [
                p.apply_async(
                    dataset_processor, args=(dataset_info, chunksize, mab20_workaround)
                )
            ]
        else:
            dataset_processor(dataset_info, chunksize, mab20_workaround)
    for x in returns:
        x.get()

    p.close()
    p.join()
