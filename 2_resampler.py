#!/usr/bin/env python3
# coding: utf-8

import sys
import os
from timeit import default_timer as timer
import multiprocessing
from typing import Optional, Callable, List
import numpy as np
import pandas as pd
import vaex
import pytz


class Datasets:
    def __init__(
        self,
        datasets: list,
        resample_period: str,
        outliers_percentile: float,
        input_path: str,
        output_path: Optional[str] = None,
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
            d["resample_period"] = resample_period
            d["outliers_percentile"] = outliers_percentile

    def as_list(self):
        return self.datasets


def fix_data_outliers_limits(
    df: pd.DataFrame, upper: float, lower: float
) -> pd.DataFrame:
    outliers = (df < lower) | (df > upper)
    df[outliers] = np.NaN
    df.interpolate(method="time", limit_area="inside")
    return df


def fix_data_outliers_iqr(df: pd.DataFrame, percentile: float) -> pd.DataFrame:
    q1 = df.quantile(percentile)
    q3 = df.quantile(1 - percentile)
    iqr = q3 - q1
    lower_limit = q1 - (1.5 * iqr)
    upper_limit = q3 + (1.5 * iqr)
    fix_data_outliers_limits(df, upper_limit, lower_limit)  # type: ignore
    return df


def process_chunk(
    df: pd.DataFrame,
    dataset_info: dict,
) -> pd.DataFrame:
    resample_period_in_seconds = (
        pd.to_timedelta(dataset_info["resample_period"]).to_numpy().astype(float) * 1e-9
    )
    sample_limit = int(max([1, 60 / resample_period_in_seconds]))

    # start_timestamp = str(
    #    pd.Timestamp(
    #        df
    #        .head(1)
    #        .index
    #        .to_numpy()[0]
    #    )
    #    .round(dataset_info['resample_period'])
    # )
    # end_timestamp = str(
    #    pd.Timestamp(
    #        df
    #        .tail(1)
    #        .index.to_numpy()[0]
    #    )
    #    .round(dataset_info['resample_period'])
    # )
    # index = pd.DatetimeIndex(
    #    pd.date_range(
    #        start=start_timestamp,
    #        end=end_timestamp,
    #        freq=dataset_info['resample_period']
    #    )
    # )
    # fix_data_outliers_iqr(
    #    df=df,
    #    percentile=dataset_info['outliers_percentile'],
    # )

    return (
        df.resample(dataset_info["resample_period"])
        .mean()
        .interpolate(method="time", limit_area="inside", limit=sample_limit)
    )  # type: ignore


def process_candump_file(
    dataset_info: dict, chunksize: int, output_file_format: str = ".hdf5", verbose=False
) -> dict:
    time_start = timer()

    input_filename = dataset_info["input_filename"]
    input_file = dataset_info["input_path"] + "/" + input_filename
    output_filename = ""

    reader = vaex.open(input_file).to_pandas_df(chunk_size=chunksize)

    total_input_lines = 0
    total_output_lines = 0
    total_time_elapsed = timer() - time_start

    for c_index, (_, _, chunk) in enumerate(reader):
        chunk_time_start = timer()

        output_filename = ".".join(input_filename.split(".")[0:2]).replace(
            "*",
            "{:03d}".format(c_index) + output_file_format,
        )
        print(output_filename)
        output_file = (
            dataset_info["output_path"]
            + "/"
            + dataset_info["resample_period"]
            + "/"
            + output_filename
        )
        if verbose:
            print("output file:    ", output_file)
        if os.path.isfile(output_file):
            print("\t -> already converted, skipping this chunk...")
            continue

        df = process_chunk(
            df=chunk.set_index("timestamp"),
            dataset_info=dataset_info,
        )

        if verbose:
            print(df.head(1).append(df.tail(1)))  # type: ignore
        if verbose:
            print(df.info(verbose=True, memory_usage="deep"))

        # Save the processed chunk to file
        vaex.from_pandas(df.reset_index()).export(output_file)

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

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        for dataset_info in dataset_info_list:
            if parallel:
                returns += [
                    pool.apply_async(dataset_processor, args=(dataset_info, chunksize))
                ]
            else:
                dataset_processor(dataset_info, chunksize)
        for x in returns:
            x.get()


def main2020():
    datasets = [
        {
            "input_filename": "candump-2020-01-29_115602.log_combined_chunk_*.hdf5",
            "description": "Prova 1: Curta do dia 2020-01-29 13:51:59-03:00",
        },
        {
            "input_filename": "candump-2020-01-30_054738.log_combined_chunk_*.hdf5",
            "description": "Prova 2: Longa do dia 2020-01-30 11:16:45-03:00",
        },
        {
            "input_filename": "candump-2020-01-30_172000.log_combined_chunk_*.hdf5",
            "description": "Prova 3: Revezamento do dia 2020-01-31 11:23:23",
        },
        {
            "input_filename": "candump-2020-02-01_064223.log_combined_chunk_*.hdf5",
            "description": "Prova 5: Curta do dia 2020-02-01 13:15:09-03:00",
        },
        {
            "input_filename": "candump-2020-02-01_064222.log_combined_chunk_*.hdf5",
            "description": "Prova 6: Slalom, e 7: Sprint",
        },
    ]

    resample_periods = [
        #         '1ms',  # More than 25 GB... Skipping it
        "10ms",
        "100ms",
        "1s",
        "10s",
        "1min",
        "5min",
    ]

    for resample_period in resample_periods:
        dataset_info_list = Datasets(
            datasets=datasets,
            input_path="../data/parsed/sparse",
            output_path="../data/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        chunksize = 1_000_000
        process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


def main2022():
    datasets = [
        {
            "input_filename": "candump-2022-03-15_205017.log_chunk_*.hdf5",
            "description": "Provas 1 e 2 - Match Race - 2022-03-17",
        },
        {
            "input_filename": "candump-2022-03-18_011750.log_chunk_*.hdf5",
            "description": "Provas 3 - Prova Longa - 2022-03-18",
        },
    ]

    resample_periods = [
        #         '1ms',  # More than 25 GB... Skipping it
        # '10ms',
        # '100ms',
        "1s",
        # '10s',
        # '1min',
        # '5min',
    ]

    for resample_period in resample_periods:
        dataset_info_list = Datasets(
            datasets=datasets,
            input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed/sparse",
            output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        chunksize = 1_000_000
        process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


def main2022_ita():
    datasets = [
        # {
        #     "input_filename": "candump-2022-10-12_?.log_chunk_*.hdf5",
        #     "description": "Match Race - 2022-10-12",
        # },
        # {
        #     "input_filename": "candump-2022-10-13_?.log_chunk_*.hdf5",
        #     "description": "Interestadual - 2022-10-13",
        # },
        # {
        #     "input_filename": "candump-2022-10-14_080650.log_chunk_*.hdf5",
        #     "description": "Prova Longa - 2022-10-14",
        # },
        # {
        #     "input_filename": "candump-2022-10-15_101750.log_chunk_*.hdf5",
        #     # "input_filename": "candump-2022-10-15_101751.log_chunk_*.hdf5",
        #     # "input_filename": "candump-2022-10-15_111751.log_chunk_*.hdf5",
        #     "description": "Prova Curta - 2022-10-15",
        # },
        {
            # "input_filename": "candump-2022-10-15_195042.log_chunk_*.hdf5",
            # "input_filename": "candump-2022-10-15_223341.log_chunk_*.hdf5",  # placas essenciais
            # "input_filename": "candump-2022-10-15_161750.log_chunk_*.hdf5",
            # "input_filename": "candump-2022-10-15_230913.log_chunk_*.hdf5",
            "input_filename": "candump-2022-10-15_234814.log_chunk_*.hdf5",
            "description": "Debugging - incrementtos de duty-cycle - 2022-10-15",
        },
    ]

    resample_periods = [
        #         '1ms',  # More than 25 GB... Skipping it
        # '10ms',
        "100ms",
        # "1s",
        # '10s',
        # '1min',
        # '5min',
    ]

    for resample_period in resample_periods:
        dataset_info_list = Datasets(
            datasets=datasets,
            input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed/sparse",
            output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
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
