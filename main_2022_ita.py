#!/usr/bin/env python3
# coding: utf-8

import os

import lib.canparser as canparser
import lib.resampler as resampler
import lib.unify_parsed_candump as unify_parsed_candump
import lib.unifier_with_forecast_data as unifier_with_forecast_data


def parse():
    schema = canparser.CanIds.load(f"{os.path.realpath('.')}/can_ids.json")
    schema = canparser.CanTopicParser.generate_parsers(schema)

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

    dataset_info_list = canparser.Datasets(
        datasets=datasets,
        input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/candump",
        output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed/sparse",
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
        dataset_info_list = resampler.Datasets(
            datasets=datasets,
            input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed/sparse",
            output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22_ita/datasets/can/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        chunksize = 1_000_000
        resampler.process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


if __name__ == "__main__":
    parse()
    resample()
