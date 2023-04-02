#!/usr/bin/env python3
# coding: utf-8


import multiprocessing
from pandas import Timestamp, Timedelta
from glob import glob

import lib.canparser as canparser
import lib.resampler as resampler
import lib.unify_parsed_candump as unify_parsed_candump
import lib.unifier_with_forecast_data as unifier_with_forecast_data


def parse():
    schema = canparser.CanIds.load("./can_ids_old.json")
    schema = canparser.CanTopicParser.generate_parsers(schema)

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
            "from": Timestamp("2020-01-29 16:51:08.332"),
            "to": Timestamp("2020-01-29 13:51:59"),
        },
        {
            "input_filename": "candump-2020-01-30_054738.log",
            "description": "Prova 2, Longa do dia 2020-01-30 11:16:45-03:00, dados incompletos (deveria ter 03:38:45)",
            "from": Timestamp("2020-01-30 10:02:30.768")
            + Timedelta("0 days 00:00:00.003666"),
            "to": Timestamp("2020-01-30 11:16:45"),
        },
        {
            "input_filename": "candump-2020-01-30_172000.log",
            "description": "Prova 3, Revezamento do dia 2020-01-31 11:23:23",
            "from": Timestamp("2020-01-30 23:32:30.720")
            + Timedelta("0 days 00:00:33.279758")
            + Timedelta("0 days 00:00:00.392470")
            + Timedelta("0 days 00:00:00.006595"),
            "to": Timestamp("2020-01-31 13:50:06.009"),
        },
        {
            "input_filename": "candump-2020-02-01_064223.log",
            "description": "Prova 5, Curta do dia 2020-02-01 13:15:09-03:00",
            "from": Timestamp("2020-02-01 09:51:06.384")
            + Timedelta("0 days 00:00:00.565093")
            - Timedelta("0 days 00:00:01.053649")
            - Timedelta("0 days 00:00:00.013652"),
            "to": Timestamp("2020-02-01 13:15:57.592"),
        },
        {
            "input_filename": "candump-2020-02-01_064222.log",
            "description": "Prova 6, Slalom, e 7, Sprint",
            "from": Timestamp("2020-02-01 11:46:58.964")
            + Timedelta("0 days 00:00:40.016623")
            + Timedelta("0 days 00:00:00.296865")
            + Timedelta("0 days 00:00:00.105090"),
            "to": Timestamp("2020-02-02 10:05:41.987"),
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

    dataset_info_list = canparser.Datasets(
        datasets=datasets,
        input_path="../data/candump",
        output_path="../data/parsed/sparse",
    ).as_list()

    chunksize = 1_000_000
    canparser.process_dataset(
        dataset_info_list,
        schema,
        chunksize=chunksize,
        parallel=True,
    )


def resample():
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
        dataset_info_list = resampler.Datasets(
            datasets=datasets,
            input_path="../data/parsed/sparse",
            output_path="../data/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        chunksize = 1_000_000
        resampler.process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


def unify():
    input_file_list = []
    input_path = "/home/joaoantoniocardoso/workspace_TCC/repo/code/data/parsed/sparse/"
    input_file_list += sorted(
        glob(input_path + "candump-2020-01-29_115602.log_chunk_*.hdf5")
    )
    input_file_list += sorted(
        glob(input_path + "candump-2020-01-30_054738.log_chunk_*.hdf5")
    )
    input_file_list += sorted(
        glob(input_path + "candump-2020-01-30_172000.log_chunk_*.hdf5")
    )
    input_file_list += sorted(
        glob(input_path + "candump-2020-02-01_064223.log_chunk_*.hdf5")
    )
    input_file_list += sorted(
        glob(input_path + "candump-2020-02-01_064222.log_chunk_*.hdf5")
    )

    file_ref = input_path + "candump-from_db*.log_chunk_*.hdf5"

    with multiprocessing.Pool(processes=8) as pool:
        for input_filename in input_file_list:
            pool.apply_async(
                unify_parsed_candump.process_file,
                args=(
                    input_filename,
                    file_ref,
                ),
            )


if __name__ == "__main__":
    parse()
    resample()
    unify()
