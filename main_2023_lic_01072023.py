#!/usr/bin/env python3
# coding: utf-8

from pandas import Timestamp, Timedelta

import lib.canparser as canparser
import lib.resampler as resampler
import lib.unify_parsed_candump as unify_parsed_candump
import lib.unifier_with_forecast_data as unifier_with_forecast_data
import lib.process_solcast_historic_data as process_solcast_historic_data
import lib.process_gpx_data as process_gpx_data

from pvlib import location


def parse():
    schema = canparser.CanIds.load("can_ids_lic_01072023.json")
    schema = canparser.CanTopicParser.generate_parsers(schema)

    datasets = [
        {
            "input_filename": "candump-2023-07-02_104810.log",
            "description": "Testes helice médio custom",
            # "from": Timestamp("2023-07-02 12:00:00.000"),
            # "to": Timestamp("2023-07-02 12:57:12.000"),
        },
    ]

    dataset_info_list = canparser.Datasets(
        datasets=datasets,
        input_path="/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/candump",
        output_path="/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/parsed/sparse",
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
            "input_filename": "candump-2023-07-02_104810.log_chunk_*.hdf5",
            "description": "Testes helice médio custom",
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
            input_path="/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/parsed/sparse",
            output_path="/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        chunksize = 1_000_000
        resampler.process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


def unify_gps():
    path = (
        "/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/strava/"
    )
    gps_files = [
        path + "Guarapuvu_II_02_07.gpx",
    ]
    gps_output_file = path + "gps_data.csv"

    process_gpx_data.process_gpx_file(gps_files, gps_output_file, "America/Sao_Paulo")

    dataset_info_list = {
        "telemetry_filename": "candump-2023-07-02_104810.log_*.hdf5",
        "telemetry_path": "/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/parsed/1s",
        "output_path": "/home/joaoantoniocardoso/ZeniteSolar/2023/can_data/01072023/datasets/can/parsed/1s",
        "gps_file": gps_output_file,
        "shift_back_localize": True,
        "period": "1s",
    }

    process_gpx_data.process_dataset(dataset_info_list)


if __name__ == "__main__":
    parse()
    resample()
    unify_gps()
