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
    schema = canparser.CanIds.load("can_ids_2022.json")
    schema = canparser.CanTopicParser.generate_parsers(schema)

    datasets = [
        {
            "input_filename": "candump-2022-03-15_205017.log",
            "description": "Provas 1 e 2 - Match Race - 2022-03-17",
            "from": Timestamp("2022-03-17 12:22:25.000"),
            "to": Timestamp("2022-03-17 09:22:31.000"),
        },
        {
            "input_filename": "candump-2022-03-18_011750.log",
            "description": "Provas 3 - Prova Longa - 2022-03-18",
            "from": Timestamp("2022-03-18 14:19:47.000"),
            "to": Timestamp("2022-03-18 11:19:47.000"),
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


def unify_forecast():
    solcast_dat_in = "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/-22.924247_-43.097405_Solcast_PT5M.csv"
    solcast_data_out = "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/nonideal_solar_dataset.csv"
    site = location.Location(
        latitude=-22.924247,
        longitude=-43.097405,
        tz="America/Sao_Paulo",
        altitude=0,
        name="Niterói",
    )

    event = {
        "time": {
            "start": "2022-03-16",
            "end": "2022-03-23",
            "freq": "5min",
        },
    }
    process_solcast_historic_data.process(solcast_dat_in, solcast_data_out, site, event)

    periods = [
        # '1ms',  # More than 25 GB... Skipping it
        # '10ms',
        # "100ms",
        "1s",
        # "10s",
        # "1m",
        # "5m",
    ]

    for period in periods:
        dataset_info_list = {
            "telemetry_filename": "candump-*.log_chunk_*.hdf5",
            "telemetry_path": "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/parsed",
            "output_path": "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/final",
            "forecast_file": solcast_data_out,
            "period": period,
            "shift_back_localize": True,
        }

        unifier_with_forecast_data.process_dataset(
            dataset_info_list,
            parallel=False,
        )


def unify_gps():
    path = "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/strava/"
    gps_files = [
        path + "17_03_2022_Prova_Manh_Classifica_o_Match_Race.gpx",
        path + "17_03_2022_Prova_Tarde_Match_Race_1.gpx",
        path + "17_03_2022_Prova_Tarde_Match_Race_2.gpx",
        path + "18_03_2022_Prova_Longa.gpx",
    ]
    gps_output_file = path + "gps_data.csv"

    process_gpx_data.process_gpx_file(gps_files, gps_output_file, "America/Sao_Paulo")

    dataset_info_list = {
        "telemetry_filename": "unified_monotonic_data_1s.hdf5",
        "telemetry_path": "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/final",
        "output_path": "/home/joaoantoniocardoso/ZeniteSolar/2022/Strategy22/datasets/can/final",
        "gps_file": gps_output_file,
        "shift_back_localize": True,
        "period": "1s",
    }

    process_gpx_data.process_dataset(dataset_info_list)


if __name__ == "__main__":
    parse()
    resample()
    unify_forecast()
    unify_gps()
