#!/usr/bin/env python
# coding: utf-8

import matplotlib.pyplot as plt
import vaex
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import multiprocessing
from glob import glob


def minmax(a: NDArray) -> tuple:
    return np.min(a), np.max(a)


def process_file(input_filename, file_ref):
    df = vaex.open(input_filename).to_pandas_df()
    dfr = vaex.open(file_ref).to_pandas_df()

    start, end = minmax(df["timestamp"].values)
    file_out = input_filename.replace("chunk", "combined_chunk")
    print(
        f"Combining file {input_filename} with {file_ref} from {start} to {end}, exporting to {file_out}."
    )

    dfr = dfr[(dfr["timestamp"] >= start) & (dfr["timestamp"] <= end)]

    df = pd.concat([df, dfr]).sort_values(by=["timestamp"])

    vaex.from_pandas(df).export(file_out)


def main2020():
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
                process_file,
                args=(
                    input_filename,
                    file_ref,
                ),
            )


if __name__ == "__main__":
    main2020()
