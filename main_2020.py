#!/usr/bin/env python3
# coding: utf-8


from glob import glob

from pandas import Timestamp
import importlib

location = importlib.import_module("pvlib.location")

import lib.canparser as canparser
import lib.process_solcast_historic_data as process_solcast_historic_data
import lib.resampler as resampler
import lib.unifier_with_forecast_data as unifier_with_forecast_data
import lib.unify_parsed_candump as unify_parsed_candump


def parse():
    schema = canparser.CanIds.load(
        "./can_ids_2020.json"
    )  # https://raw.githubusercontent.com/ZeniteSolar/CAN_IDS/28e5341e61175fe1e4490c13202b89ca6375ccac/can_ids.json
    schema = canparser.CanTopicParser.generate_parsers(schema)

    datasets = [
        {
            "input_filename": "candump-2019-08-14_173441.log",
        },
        {
            "input_filename": "candump-2019-08-14_201210.log",
        },
        {
            "input_filename": "candump-2019-08-14_201641.log",
        },
        {
            "input_filename": "candump-2019-09-06_164805.log",
        },
        {
            "input_filename": "candump-2019-09-20_124423.log",
        },
        {
            "input_filename": "candump-2019-09-20_132408.log",
        },
        {
            "input_filename": "candump-2020-01-14_224047.log",
        },
        {
            "input_filename": "candump-2020-01-25_171104.log",
        },
        {
            "input_filename": "candump-2020-01-25_192039.log",
        },
        {
            "input_filename": "candump-2020-01-27_040133.log",
        },
        {
            "input_filename": "candump-2020-01-29_111700.log",
        },
        {
            "input_filename": "candump-2020-01-29_114446.log",
        },
        {
            "input_filename": "candump-2020-01-29_154348.log",
        },
        {
            "input_filename": "candump-2020-01-30_054740.log",
        },
        {
            "input_filename": "candump-2020-01-30_171953.log",
        },
        {
            "input_filename": "candump-2020-01-30_171958.log",
        },
        {
            "input_filename": "candump-2020-01-30_171959.log",
        },
        {
            "input_filename": "candump-2020-02-01_002021.log",
        },
        {
            "input_filename": "candump-2020-02-01_064221.log",
        },
        {
            "input_filename": "candump-2020-01-29_115602.log",
            "description": "Prova 1, Curta do dia 2020-01-29 13:51:59-03:00",
            "from": Timestamp("2020-01-29 16:51:08.332", tz="UTC"),
            "to": Timestamp("2020-01-29 13:51:59", tz="America/Sao_Paulo"),
        },
        {
            "input_filename": "candump-2020-01-30_054738.log",
            "description": "Prova 2, Longa do dia 2020-01-30 11:16:45-03:00, dados incompletos (deveria ter 03:38:45)",
            "from": Timestamp("2020-01-30 10:02:30.771666", tz="UTC"),
            "to": Timestamp("2020-01-30 11:16:45", tz="America/Sao_Paulo"),
        },
        {
            "input_filename": "candump-2020-01-30_172000.log",
            "description": "Prova 3, Revezamento do dia 2020-01-31 11:23:23",
            "from": Timestamp("2020-01-30 23:33:04.398823", tz="UTC"),
            "to": Timestamp("2020-01-31 13:50:06.009", tz="America/Sao_Paulo"),
        },
        {
            "input_filename": "candump-2020-02-01_064223.log",
            "description": "Prova 5, Curta do dia 2020-02-01 13:15:09-03:00",
            "from": Timestamp("2020-02-01 09:51:05.881792", tz="UTC"),
            "to": Timestamp("2020-02-01 13:15:57.592", tz="America/Sao_Paulo"),
        },
        {
            "input_filename": "candump-2020-02-01_064222.log",
            "description": "Prova 6, Slalom, e 7, Sprint",
            "from": Timestamp("2020-02-01 11:47:39.382578", tz="UTC"),
            "to": Timestamp("2020-02-02 10:05:41.987", tz="America/Sao_Paulo"),
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
        mab20_workaround=True,
    )


def unify():
    input_file_list = []
    input_path = "../data/parsed/sparse/"
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

    unify_parsed_candump.process_dataset(input_file_list, file_ref, parallel=True)


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
        "50ms",
        "100ms",
        "1s",
        "10s",
    ]

    resample_agg_rules = [
        {"pattern": r"__STATE__", "agg": "last"},
        {"pattern": r"(?:^|__)ON(?:__|$)", "agg": "last"},
        {"pattern": r"MOTOR__MOTOR", "agg": "last"},
        {"pattern": r"__D$", "agg": "mean"},
        {"pattern": r"ADC__AVG$", "agg": "mean"},
        {"pattern": r"BAT__AVG$", "agg": "mean"},
        {"pattern": r"CAP__AVG$", "agg": "mean"},
        {"pattern": r"RPM__AVG$", "agg": "mean"},
    ]

    fill_method_rules = [
        {"pattern": r"__STATE__", "method": "ffill"},
        {"pattern": r"(?:^|__)ON(?:__|$)", "method": "ffill"},
        {"pattern": r"MOTOR__MOTOR", "method": "ffill"},
        {"pattern": r"__D$", "method": "ffill"},
    ]

    for resample_period in resample_periods:
        dataset_info_list = resampler.Datasets(
            datasets=datasets,
            input_path="../data/parsed/sparse",
            output_path="../data/parsed",
            outliers_percentile=0.01,
            resample_period=resample_period,
        ).as_list()

        if resample_period == "50ms":
            fill_limit_seconds = 0.25
        elif resample_period == "100ms":
            fill_limit_seconds = 0.5
        else:
            fill_limit_seconds = 2.0

        for d in dataset_info_list:
            d["resample_agg"] = "mean"
            d["fill_method"] = "interpolate"
            d["fill_limit_seconds"] = fill_limit_seconds
            d["resample_agg_rules"] = resample_agg_rules
            d["fill_method_rules"] = fill_method_rules
            d["filtfilt_rules"] = [
                {
                    "patterns": [r"RPM__AVG$"],
                    "cutoff_hz": 0.4,
                    "order": 2,
                    "clip_min": 0.0,
                },
                {
                    "patterns": [
                        r"^MCC19_.*__MEASUREMENTS__",
                        r"^MCB19_.*__MEASUREMENTS__",
                    ],
                    "cutoff_hz": 1.0,
                    "order": 2,
                },
            ]

        chunksize = 1_000_000
        resampler.process_dataset(
            dataset_info_list,
            chunksize=chunksize,
            parallel=True,
        )


def unify_forecast():
    timezone = "America/Sao_Paulo"

    solcast_dat_in = "../data/solcast/-26.243602_-48.6417668_Solcast_PT5M.csv"
    solcast_data_out = "../data/solcast/nonideal_solar_dataset.csv"
    site = location.Location(
        latitude=-26.243602,
        longitude=-48.6417668,
        tz=timezone,
        altitude=0,
        name="São Francisco do Sul",
    )
    event = {
        "time": {
            "start": "2020-01-29",
            "end": "2020-02-02",
            "freq": "5min",
        },
    }
    process_solcast_historic_data.process(solcast_dat_in, solcast_data_out, site, event)

    periods = [
        "100ms",
        "50ms",
        "1s",
        "10s",
    ]

    for period in periods:
        dataset_info_list = {
            "telemetry_filename": "candump-*.log_combined_chunk_*.hdf5",
            "telemetry_path": "../data/parsed",
            "output_path": "../data/final",
            "forecast_file": solcast_data_out,
            "period": period,
            "timezone": timezone,
        }

        unifier_with_forecast_data.process_dataset(
            dataset_info_list,
            parallel=True,
        )


if __name__ == "__main__":
    # parse()
    # unify()
    # resample()
    unify_forecast()
