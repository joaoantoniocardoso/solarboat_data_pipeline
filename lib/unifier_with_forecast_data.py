import os
from timeit import default_timer as timer
import multiprocessing
from typing import Optional, List
import numpy as np
import pandas as pd
import vaex
import pytz


def process_candump_file(
    dataset_info: dict, output_file_format: str = ".hdf5", verbose=False
) -> Optional[dict]:
    time_start = timer()

    # Create the filenames, check if it was already proccessed
    telemetry_filename = dataset_info["telemetry_filename"]
    telemetry_file = (
        dataset_info["telemetry_path"]
        + "/"
        + dataset_info["period"]
        + "/"
        + telemetry_filename
    )
    output_filename = (
        "unified_monotonic_data_" + dataset_info["period"] + output_file_format
    )
    print(output_filename)
    output_file = dataset_info["output_path"] + "/" + output_filename
    if verbose:
        print("output file:    ", output_file)
    if os.path.isfile(output_file):
        print("\t -> already converted, skipping this chunk...")
        return None

    # Load telemetry data, and reindex as a constant-frequency/monotonic timeseries
    df_telemetry = vaex.open(telemetry_file)
    df_telemetry = df_telemetry.to_pandas_df().set_index("timestamp")
    df_telemetry = df_telemetry[~df_telemetry.index.duplicated()]
    df_telemetry = (
        df_telemetry.asfreq(dataset_info["period"])
        .tz_localize(pytz.timezone("America/Sao_Paulo"))
        .reset_index()
    )
    df_telemetry = vaex.from_pandas(df_telemetry)

    # Load and proccess Solar Data
    df_forecast = pd.read_csv(
        dataset_info["forecast_file"],
        index_col="PeriodStart",
        parse_dates=["PeriodStart"],
        infer_datetime_format=True,
    ).add_prefix("solcast_")
    df_forecast.index.rename("timestamp", inplace=True)

    # Reindex solar data using the telemetry timestamp as index
    df_forecast.index = df_forecast.reset_index()["timestamp"].dt.tz_convert(
        pytz.timezone("America/Sao_Paulo")
    )  # type: ignore
    index = pd.DatetimeIndex(
        df_telemetry["timestamp"].to_numpy(), dtype="datetime64[ns, America/Sao_Paulo]"  # type: ignore
    ) - pd.Timedelta(3, unit="H")

    df_forecast = df_forecast.reindex(
        index=index,
        method="ffill",
        copy=False,
    )
    df_forecast["timestamp"] = df_forecast.index
    df_forecast = vaex.from_pandas(df_forecast.reset_index(drop=True))

    # Unify the data
    df = df_telemetry.join(
        df_forecast,
        on="timestamp",
        how="left",
        rprefix="solcast_",
        allow_duplication=True,
    )

    if verbose:
        print(df.head(1).append(df.tail(1)))
    if verbose:
        print(df.info())

    df.export(output_file)

    time_end = timer()
    time_elapsed = time_end - time_start
    input_lines = len(df_telemetry) + len(df_forecast)  # type: ignore
    output_lines = len(df)

    return {
        "Input File Name": telemetry_filename,
        "Output File Name": output_filename,
        "Elapsed time": time_elapsed,
        "Input lines": input_lines,
        "Output lines": output_lines,
    }


def dataset_processor(
    dataset_info: dict,
):
    print("Processing file:", dataset_info["telemetry_filename"])

    report = process_candump_file(dataset_info)
    if report:
        report_str = [
            "-" * 80 + "\n",
            f"Finished Input File: {dataset_info['telemetry_filename']}\n",
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


def process_dataset(dataset_info: dict, parallel: bool = True):
    returns = []
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        if parallel:
            returns += [pool.apply_async(dataset_processor, args=(dataset_info,))]
        else:
            dataset_processor(dataset_info)
        for x in returns:
            x.get()


def main():
    periods = [
        #         '1ms',  # More than 25 GB... Skipping it
        #'10ms',
        "100ms",
        "1s",
        "10s",
        "1m",
        "5m",
    ]

    for period in periods:
        dataset_info_list = {
            "telemetry_filename": "candump-*.log_combined_chunk_*.hdf5",
            "telemetry_path": "../data/parsed",
            "output_path": "../data/final",
            "forecast_file": "../models/Competition/datasets/nonideal_solar_dataset.csv",
            "period": period,
        }

        process_dataset(
            dataset_info_list,
            parallel=False,
        )


if __name__ == "__main__":
    main()
