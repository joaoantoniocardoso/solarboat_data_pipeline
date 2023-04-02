#!/usr/bin/env python3
# coding: utf-8


import lib.canparser as canparser
import lib.resampler as resampler
import lib.unify_parsed_candump as unify_parsed_candump
import lib.unifier_with_forecast_data as unifier_with_forecast_data


def parse():
    schema = canparser.CanIds.load("can_ids_old.json")
    schema = canparser.CanTopicParser.generate_parsers(schema)

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

    dataset_info_list = canparser.Datasets(
        datasets=datasets,
        input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/candump",
        output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed/sparse",
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
        dataset_info_list = resampler.Datasets(
            datasets=datasets,
            input_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed/sparse",
            output_path="/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed",
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
