import multiprocessing
import os
from typing import List

import importlib

vaex = importlib.import_module("vaex")


def process_file(input_filename: str, file_ref: str) -> None:
    df = vaex.open(input_filename)
    dfr = vaex.open(file_ref)

    start, end = df.minmax("timestamp")
    file_out = input_filename.replace("chunk", "combined_chunk")
    print(
        f"Combining file {input_filename} with {file_ref} from {start} to {end}, exporting to {file_out}."
    )

    dfr = dfr[(dfr["timestamp"] >= start) & (dfr["timestamp"] <= end)]

    df_out = vaex.concat([df, dfr]).sort("timestamp")
    os.makedirs(os.path.dirname(file_out) or ".", exist_ok=True)
    df_out.export(file_out)


def process_dataset(
    input_filename_list: List[str], file_ref: str, parallel: bool = False
):
    if not parallel:
        for input_filename in input_filename_list:
            process_file(input_filename, file_ref)
        return

    returns = []
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        for input_filename in input_filename_list:
            returns += [pool.apply_async(process_file, args=(input_filename, file_ref))]
        for x in returns:
            x.get()
