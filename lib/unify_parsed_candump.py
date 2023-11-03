import vaex
import numpy as np
from numpy.typing import NDArray
import multiprocessing
from typing import List
import pandas as pd


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


def process_dataset(
    input_filename_list: List[str], file_ref: str, parallel: bool = True
):
    returns = []

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        for input_filename in input_filename_list:
            if parallel:
                returns += [
                    pool.apply_async(process_file, args=(input_filename, file_ref))
                ]
            else:
                process_file(input_filename, file_ref)
        for x in returns:
            x.get()
