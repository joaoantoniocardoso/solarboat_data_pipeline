import vaex
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import multiprocessing

from typing import List


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
