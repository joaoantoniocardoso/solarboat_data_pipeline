import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pytz import timezone
import vaex
from timeit import default_timer as timer
from typing import Optional, List
import os

from gpx_converter import Converter


def compute_heading(lat1, lon1, lat2, lon2):
    # Reference: https://github.com/kgodden/calculate_gps_heading

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1

    x = np.cos(lat1) * np.sin(dlon)
    y = np.sin(lat1) * (np.cos(lat1) - np.cos(lat2) * np.cos(dlon))

    heading = np.rad2deg(np.arctan2(x, y)) + 180.0
    return heading


def compute_haversine(lat1, lon1, lat2, lon2):
    """Returns the distance between the points in km. Vectorized and will work with arrays and return an array of
    distances, but will also work with scalars and return a scalar.

    Reference: https://www.tjansson.dk/2021/03/vectorized-gps-distance-speed-calculation-for-pandas/
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2.0) ** 2 + (
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0) ** 2
    )
    distance = 6371 * 2 * np.arcsin(np.sqrt(a))
    return distance


def process_gps(longitudes: pd.Series, latitudes: pd.Series, timestamps: pd.Series):
    """
    Rererence: https://www.tjansson.dk/2021/03/vectorized-gps-distance-speed-calculation-for-pandas/
    """

    assert longitudes.shape[0] > 1
    assert latitudes.shape[0] > 1
    assert timestamps.shape[0] > 1

    lat1 = latitudes.values[:-1]
    lon1 = longitudes.values[:-1]
    lat2 = latitudes.values[1:]
    lon2 = longitudes.values[1:]

    # Vectorized haversine calculation
    distance = compute_haversine(lat1, lon1, lat2, lon2)
    time = (timestamps.diff().dt.seconds / 3600).values[1:]

    # Calculate the speed
    time[time == 0] = np.nan  # To avoid division by zero
    speed = distance / time

    # Calculate heading
    heading = compute_heading(lat1, lon1, lat2, lon2)

    # Make the arrays as long as the input arrays
    speed = np.insert(speed, 0, np.nan, axis=0)
    heading = np.insert(heading, 0, np.nan, axis=0)
    distance = np.insert(distance, 0, np.nan, axis=0)

    # Total distance in km
    distance = np.nancumsum(distance)

    return heading, distance, speed


def process_gpx_file(
    input_files,
    output_file,
    timezone="America/Sao_Paulo",
):
    df_full = pd.DataFrame()
    for file in input_files:
        df = Converter(input_file=file).gpx_to_dataframe()

        df = df.rename(columns={"time": "timestamp"})

        df["timestamp"] = pd.DatetimeIndex(df["timestamp"]).tz_convert(timezone(timezone))  # type: ignore

        heading, distance, speed = process_gps(
            df["longitude"], df["latitude"], df["timestamp"]
        )
        df["speed"] = speed
        df["heading"] = heading
        df["distance"] = distance

        df = df.set_index("timestamp").dropna()

        df_full = pd.concat([df_full, df])

    df_full.to_csv(output_file)

    return df_full


def process_dataset(
    dataset_info: dict,
    output_file_format: str = ".hdf5",
    verbose=False,
) -> Optional[dict]:
    time_start = timer()

    # Create the filenames, check if it was already proccessed
    telemetry_filename = dataset_info["telemetry_filename"]
    telemetry_file = dataset_info["telemetry_path"] + "/" + telemetry_filename
    output_filename = (
        "unified_monotonic_data_"
        + dataset_info["period"]
        + "_with_gps"
        + output_file_format
    )
    print(output_filename)
    output_file = dataset_info["output_path"] + "/" + output_filename
    if verbose:
        print("output file:    ", output_file)
    if os.path.isfile(output_file):
        print("\t -> already converted, skipping this chunk...")
        return None

    # Load telemetry data, and reindex as a constant-frequency/monotonic timeseries
    df_telemetry = vaex.open(telemetry_file).to_pandas_df()

    # Localize the timestamp
    tzinfo = timezone("America/Sao_Paulo")
    df_telemetry["timestamp"] = pd.to_datetime(df_telemetry["timestamp"])
    df_telemetry["timestamp"] = df_telemetry["timestamp"].dt.tz_localize(
        timezone("UTC")
    )
    df_telemetry["timestamp"] = df_telemetry["timestamp"].dt.tz_convert(tzinfo)
    df_telemetry = vaex.from_pandas(df_telemetry)

    # Load and proccess GPS Data
    df_gps = (
        pd.read_csv(
            dataset_info["gps_file"],
            index_col="timestamp",
            parse_dates=["timestamp"],
            infer_datetime_format=True,
        )
        .add_prefix("gps_")
        .dropna()
    )
    df_gps.index.rename("timestamp", inplace=True)

    # Reindex GPS data using the telemetry timestamp as index
    df_gps.index = df_gps.reset_index()["timestamp"].dt.tz_convert(
        timezone("America/Sao_Paulo")
    )  # type: ignore
    index = pd.DatetimeIndex(
        df_telemetry["timestamp"].to_numpy(), dtype="datetime64[ns, America/Sao_Paulo]"  # type: ignore
    )

    if dataset_info["shift_back_localize"]:
        index -= pd.Timedelta(3, unit="H")

    df_gps = df_gps.reindex(
        index=index,
        method="ffill",
        copy=False,
    )
    df_gps["timestamp"] = df_gps.index
    df_gps = vaex.from_pandas(df_gps.reset_index(drop=True))

    # Unify the data
    df = df_telemetry.join(
        df_gps,
        on="timestamp",
        how="left",
        rprefix="gps_",
        allow_duplication=True,
    )

    if verbose:
        print(df.head(1).append(df.tail(1)))
    if verbose:
        print(df.info())

    df.export(output_file)

    time_end = timer()
    time_elapsed = time_end - time_start
    input_lines = len(df_telemetry) + len(df_gps)  # type: ignore
    output_lines = len(df)

    return {
        "Input File Name": telemetry_filename,
        "Output File Name": output_filename,
        "Elapsed time": time_elapsed,
        "Input lines": input_lines,
        "Output lines": output_lines,
    }
