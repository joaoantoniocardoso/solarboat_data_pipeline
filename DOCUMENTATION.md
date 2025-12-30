# Solar Boat Data Pipeline — Processing Documentation

This document describes every data-processing step implemented in `lib/` and how the entry-point scripts orchestrate them. Each section links to the exact file that implements the behavior.

## Pipeline Overview (Entry Points)

- `main_2020.py`: full pipeline for 2020 data (CAN parsing → unification → resampling → solar forecast merge). Uses `lib/canparser.py`, `lib/unify_parsed_candump.py`, `lib/resampler.py`, `lib/process_solcast_historic_data.py`, `lib/unifier_with_forecast_data.py`.
- `main_2022.py`: 2022 pipeline (CAN parsing → resampling → solar forecast merge → GPS merge). Uses the same core libs plus `lib/process_gpx_data.py`.
- `main_2023_lic_01072023.py`: 2023 pipeline (CAN parsing → resampling → GPS merge).
- `main_2022_ita.py`: alternate 2022 pipeline focused on parsing and resampling.

## Data Conversion (CSV/JSON → candump)

- **File:** `convert_json_to_candump.py`
- **Purpose:** Convert raw telemetry exports (CSV/JSON) into standard candump log lines.
- **Steps:**
  1. `convert_csv_to_json_file(...)` reads CSV columns (`date`, `top`, `mod`, `bytes`), sorts by time, and saves JSON records.
  2. `convert_json_db_to_candump_log_row(...)`:
     - Converts `date` to UNIX timestamp (float seconds).
     - Formats `top` (topic) as 3-hex digits and `mod` (module) as 2-hex digits.
     - Concatenates module + payload bytes into `payload` and outputs `(timestamp) can0 TTT#PAYLOAD`.
  3. `convert_json_db_to_candump_log_file(...)` streams JSON records, converts each to a candump row, and writes a `.log` file.

## CAN Schema & Binary Parsing

- **File:** `lib/canparser_generator.py`
- **Purpose:** Build ctypes parsers from `can_ids_*.json` schema definitions.
- **Steps:**
  1. `CanTopicParser.generate_parsers(...)` iterates all modules/topics and attaches a `ctypes` parser to each topic.
  2. `CanTopicParser.create(...)` creates a packed `ctypes.LittleEndianStructure` with fields inferred from each topic’s byte definitions.
  3. `apply_units(...)` decodes unit strings like `%` or `A/100`, scaling raw integer values into engineering units.

## CAN Log Parsing (candump → sparse HDF5)

- **File:** `lib/canparser.py`
- **Purpose:** Parse candump log lines into structured, unit-correct telemetry tables.
- **Key steps:**
  1. **Schema load:** `CanIds.load(...)` reads `can_ids_*.json` and builds fast lookup maps for module/topic IDs.
  2. **Regex extraction:** `process_candump_file(...)` scans each chunk with a regex to extract `timestamp`, `topic` and `payload` bytes.
  3. **Timestamp alignment:** `process_chunk(...)` converts timestamps to `datetime`, applies dataset offsets (`from`/`to` windows), and trims out-of-range rows.
  4. **Payload decoding:** `process_message(...)`:
     - Applies optional `mab20_workaround` remapping for known module/topic bugs.
     - Locates the module/topic in the schema.
     - Calls `parse_payload(...)`, which uses the ctypes topic parser to decode bytes and apply unit scaling.
  5. **Reshape to wide format:** After parsing, data is grouped by `[unit, byte_name, topic_name, module_name, timestamp]`, averaged, and unstacked to a wide DataFrame with columns like `unit__byte__topic__module`.
  6. **Downcast + export:** Data is downcast to `float16`, converted to Vaex, and saved as chunked `.hdf5` files.
  7. **Timestamp cleanup:** `clean_timestamp_outliers(...)` removes large timestamp jumps (unless the file is a `db*` import).

## Unify Parsed candump (merge reference data)

- **File:** `lib/unify_parsed_candump.py`
- **Purpose:** Merge parsed chunks with a reference dataset in a matching time range.
- **Steps:**
  1. Load the chunk file and a reference file into pandas via Vaex.
  2. Compute start/end timestamps for the chunk.
  3. Crop reference rows to that same time window.
  4. Concatenate, sort by `timestamp`, and export to a new `*_combined_chunk_*.hdf5` file.

## Resampling (sparse → fixed-rate)

- **File:** `lib/resampler.py`
- **Purpose:** Resample telemetry to a constant interval (e.g., `100ms`, `1s`).
- **Steps:**
  1. `process_candump_file(...)` reads each HDF5 chunk via Vaex in pandas chunks.
  2. `process_chunk(...)` resamples on `timestamp` using `.resample(period).first()` (no interpolation by default).
  3. Output files are written to `<output_path>/<period>/...` as `.hdf5`.
- **Optional utilities:** `fix_data_outliers_iqr(...)` and `fix_data_outliers_limits(...)` are provided but currently not active in the pipeline.

## Solar Forecast Processing (Solcast → model-ready)

- **File:** `lib/process_solcast_historic_data.py`
- **Purpose:** Convert Solcast historic CSV into a consistent, model-ready solar dataset.
- **Steps:**
  1. Read CSV and normalize column names to `dni`, `ghi`, `dhi`, `albedo`.
  2. Localize index to the site timezone and infer frequency.
  3. `get_irradiance(...)` computes plane-of-array (POA) irradiance using PVlib (with mid-interval timeshift).
  4. Restrict data to the event window, then `integrate(...)` to compute cumulative solar energy.
  5. Save the processed forecast file as CSV.

## Telemetry + Solar Forecast Unification

- **File:** `lib/unifier_with_forecast_data.py`
- **Purpose:** Join resampled telemetry with processed Solcast forecast data.
- **Steps:**
  1. Load telemetry HDF5 and reindex to a strict fixed frequency using `.asfreq(period)`.
  2. Localize timestamps to `America/Sao_Paulo`.
  3. Load the forecast CSV, prefix columns with `solcast_`, and reindex to telemetry timestamps.
  4. Interpolate forecast values and join on `timestamp`.
  5. Export unified data as `unified_monotonic_data_<period>.hdf5`.

## GPS Processing & Merge

- **File:** `lib/process_gpx_data.py`
- **Purpose:** Convert GPX tracks to a telemetry-aligned dataset and merge with CAN telemetry.
- **Steps:**
  1. `process_gpx_file(...)` converts GPX files to a dataframe and localizes timestamps.
  2. `process_gps(...)` computes speed, heading, and cumulative distance using haversine geometry.
  3. Save GPS data to CSV for reuse.
  4. `process_dataset(...)` loads telemetry, localizes to `America/Sao_Paulo`, reindexes GPS data to telemetry timestamps (forward fill), and joins with `gps_`-prefixed fields.
  5. Export a unified HDF5 file named `unified_monotonic_data_<period>_with_gps.hdf5`.

## How to Trace Any Data Product

1. Identify the entry script (`main_*.py`) that generated the dataset.
2. Follow the pipeline stages above in order; each stage writes its own output directory (`parsed/sparse` → `parsed/<period>` → `final`).
3. Use filenames as checkpoints: `*_chunk_*.hdf5` (parsed), `*_combined_chunk_*.hdf5` (unified candump), `unified_monotonic_data_*.hdf5` (forecast-merged), and `*_with_gps.hdf5` (GPS-merged).
