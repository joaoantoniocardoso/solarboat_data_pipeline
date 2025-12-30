import os
from timeit import default_timer as timer
import multiprocessing
import re
from typing import Optional, Callable, List, Dict, Any, cast
import numpy as np
import pandas as pd
import importlib

vaex = importlib.import_module("vaex")


class Datasets:
    def __init__(
        self,
        datasets: list,
        resample_period: str,
        outliers_percentile: float,
        input_path: str,
        output_path: Optional[str] = None,
    ):
        if output_path is None:
            output_path = input_path

        self.datasets = datasets

        for d in self.datasets:
            d["input_path"] = input_path
            d["output_path"] = output_path
            d["resample_period"] = resample_period
            d["outliers_percentile"] = outliers_percentile

    def as_list(self):
        return self.datasets


def fix_data_outliers_limits(
    df: pd.DataFrame, upper: float, lower: float
) -> pd.DataFrame:
    outliers = (df < lower) | (df > upper)
    df[outliers] = np.nan
    df.interpolate(method="time", limit_area="inside")
    return df


def fix_data_outliers_iqr(df: pd.DataFrame, percentile: float) -> pd.DataFrame:
    q1 = df.quantile(percentile)
    q3 = df.quantile(1 - percentile)
    iqr = q3 - q1
    lower_limit = q1 - (1.5 * iqr)
    upper_limit = q3 + (1.5 * iqr)
    fix_data_outliers_limits(df, upper_limit, lower_limit)  # type: ignore
    return df


def _compile_rules(rules: Any) -> List[Dict[str, Any]]:
    if not rules:
        return []
    if isinstance(rules, dict):
        rules = [rules]
    compiled = []
    for rule in rules:
        compiled.append({**rule, "_re": re.compile(rule["pattern"])})
    return compiled


def _match_rule(
    name: str, compiled_rules: List[Dict[str, Any]], key: str
) -> Optional[str]:
    for rule in compiled_rules:
        if rule["_re"].search(name):
            return rule.get(key)
    return None


def process_chunk(
    df: pd.DataFrame,
    dataset_info: dict,
) -> pd.DataFrame:
    period = dataset_info["resample_period"]
    default_agg = dataset_info.get("resample_agg", "first")
    default_fill_method = dataset_info.get("fill_method", "ffill")

    agg_rules = _compile_rules(dataset_info.get("resample_agg_rules"))
    fill_rules = _compile_rules(dataset_info.get("fill_method_rules"))

    r = df.resample(period)

    if agg_rules:
        agg_map = {}
        for col in df.columns:
            col_agg = _match_rule(col, agg_rules, "agg") or default_agg
            agg_map[col] = col_agg
        df_resampled = r.agg(agg_map)
    else:
        if default_agg == "first":
            df_resampled = r.first()
        elif default_agg == "last":
            df_resampled = r.last()
        elif default_agg == "mean":
            df_resampled = r.mean()
        elif default_agg == "median":
            df_resampled = r.median()
        else:
            raise ValueError(f"Unsupported resample_agg: {default_agg}")

    fill_limit_seconds = float(dataset_info.get("fill_limit_seconds", 1.0))
    period_seconds = float(pd.to_timedelta(period).total_seconds())
    fill_limit = int(max(0, fill_limit_seconds / period_seconds))

    df_out = df_resampled  # type: ignore

    if fill_limit > 0:
        if not fill_rules and default_fill_method:
            if default_fill_method == "ffill":
                df_out = df_out.ffill(limit=fill_limit)
            elif default_fill_method == "interpolate":
                df_out = df_out.interpolate(
                    method="time",
                    limit_area="inside",
                    limit=fill_limit,
                )
            else:
                raise ValueError(f"Unsupported fill_method: {default_fill_method}")
        else:
            df_ffill = df_out.ffill(limit=fill_limit)
            df_interp = df_out.interpolate(
                method="time",
                limit_area="inside",
                limit=fill_limit,
            )

            cols_ffill = []
            cols_interp = []
            for col in df_out.columns:
                method = _match_rule(col, fill_rules, "method") or default_fill_method
                if method == "interpolate":
                    cols_interp.append(col)
                elif method == "ffill":
                    cols_ffill.append(col)
                elif not method:
                    pass
                else:
                    raise ValueError(f"Unsupported fill_method: {method}")

            df_out = df_out.copy()
            if cols_ffill:
                df_out[cols_ffill] = df_ffill[cols_ffill]
            if cols_interp:
                df_out[cols_interp] = df_interp[cols_interp]

    filtfilt_rules = dataset_info.get("filtfilt_rules")

    if filtfilt_rules:
        signal = importlib.import_module("scipy.signal")
        fs_hz = 1.0 / period_seconds

        for rule in filtfilt_rules:
            patterns = rule.get("patterns", [])
            if isinstance(patterns, str):
                patterns = [patterns]
            compiled = [re.compile(p) for p in patterns]

            cutoff_hz = float(rule.get("cutoff_hz", 0.0))
            if cutoff_hz <= 0:
                continue

            order = int(rule.get("order", 2))
            clip_min = rule.get("clip_min")

            max_cutoff_hz = 0.45 * fs_hz
            effective_cutoff_hz = min(cutoff_hz, max_cutoff_hz)
            wn = effective_cutoff_hz / (0.5 * fs_hz)
            if not (wn > 0):
                continue

            b, a = signal.butter(order, min(wn, 0.999), btype="low")

            cols = [c for c in df_out.columns if any(r.search(c) for r in compiled)]
            if not cols:
                continue

            df_out = df_out.copy()
            for col in cols:
                s = pd.Series(
                    pd.to_numeric(df_out[col], errors="coerce"), index=df_out.index
                )

                if fill_limit > 0:
                    s = s.interpolate(  # type: ignore
                        method="time", limit_area="inside", limit=fill_limit
                    )
                else:
                    s = s.interpolate(method="time", limit_area="inside")  # type: ignore
                s = s.ffill().bfill()

                x = s.to_numpy(dtype=float)
                if x.size < 4:
                    continue

                default_padlen = 3 * (max(len(a), len(b)) - 1)
                padlen = int(min(default_padlen, x.size - 1))
                y = signal.filtfilt(b, a, x, padlen=padlen)

                if clip_min is not None:
                    y = np.maximum(y, float(clip_min))

                df_out[col] = y

    return df_out  # type: ignore


def process_candump_file(
    dataset_info: dict, chunksize: int, output_file_format: str = ".hdf5", verbose=False
) -> dict:
    time_start = timer()

    input_filename = dataset_info["input_filename"]
    input_file = dataset_info["input_path"] + "/" + input_filename
    output_filename = ""

    reader = vaex.open(input_file).to_pandas_df(chunk_size=chunksize)

    total_input_lines = 0
    total_output_lines = 0
    total_time_elapsed = timer() - time_start

    for c_index, (_, _, chunk) in enumerate(reader):
        chunk_time_start = timer()

        output_filename = ".".join(input_filename.split(".")[0:2]).replace(
            "*",
            "{:03d}".format(c_index) + output_file_format,
        )
        print(output_filename)
        output_file = (
            dataset_info["output_path"]
            + "/"
            + dataset_info["resample_period"]
            + "/"
            + output_filename
        )
        os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
        if verbose:
            print("output file:    ", output_file)
        if os.path.isfile(output_file):
            print("\t -> already converted, skipping this chunk...")
            continue

        df = process_chunk(
            df=chunk.set_index("timestamp"),
            dataset_info=dataset_info,
        )

        if verbose:
            print(df.head(1).append(df.tail(1)))  # type: ignore
        if verbose:
            print(df.info(verbose=True, memory_usage="deep"))

        # Save the processed chunk to file
        vaex.from_pandas(df.reset_index()).export_hdf5(output_file)

        chunk_time_end = timer()
        chunk_time_elapsed = chunk_time_end - chunk_time_start
        chunk_input_lines = len(chunk)
        chunk_output_lines = len(df)
        if verbose:
            print(
                *[
                    f"Chunk {c_index},",
                    f"elapsed: {chunk_time_elapsed} s,",
                    f"output/input: {chunk_output_lines}/{chunk_input_lines} lines.",
                ]
            )

        total_input_lines += chunk_input_lines
        total_output_lines += chunk_output_lines
        total_time_elapsed += chunk_time_elapsed

    return {
        "Input File Name": input_filename,
        "Output File Name": output_filename,
        "Elapsed time": total_time_elapsed,
        "Input lines": total_input_lines,
        "Output lines": total_output_lines,
    }


def dataset_processor(
    dataset_info: dict,
    chunksize: int,
):
    print("Processing file:", dataset_info["input_filename"])

    report = process_candump_file(dataset_info, chunksize)

    report_str = [
        "-" * 80 + "\n",
        f"Finished Input File: {dataset_info['input_filename']}\n",
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


def process_dataset(
    dataset_info_list: List[dict], chunksize: int, parallel: bool = True
):
    returns = []

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        for dataset_info in dataset_info_list:
            if parallel:
                returns += [
                    pool.apply_async(dataset_processor, args=(dataset_info, chunksize))
                ]
            else:
                dataset_processor(dataset_info, chunksize)
        for x in returns:
            x.get()
