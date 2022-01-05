#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
import pandas as pd
import ijson
import json
from io import BytesIO
from tqdm.autonotebook import tqdm

def test_convert_json_db_to_candump_log_row(debug=True):
    # Example of the data to be read from the json file:
    f = BytesIO(b"""
    [
        {
            "bytes":["159","4","246","0","246","0"],
            "_id":"5e46eaa88526db167da18772",
            "date":"2020-02-14 15:44:54.944",
            "mod":250,
            "top":33,
            "__v":0
        }
    ]
    """)
    j = list(ijson.items(f, ''))[0]
    element = j[0]

    print('#' * 80)
    print('Test: Single element conversion')
    print(f'\n\t{element = }')

    expected_output = '(1581695094.944000) can0 021#fa9f04f600f600'
    print(f'\n\t{expected_output = }')

    output = convert_json_db_to_candump_log_row(element, debug=debug)
    print(f'\t\t {output = }')

    print('Result: ', end='')
    assert(output == expected_output)
    print('Pass!')
    print('#' * 80)


def convert_json_db_to_candump_log_row(element: dict, debug=False) -> str:
    """
    Example of input dict element:
        {
            "bytes":["159","4","246","0","246","0"],
            "_id":"5e46eaa88526db167da18772",
            "date":"2020-02-14 15:44:54.944",
            "mod":250,
            "top":33,
            "__v":0
        }
    Example of output candump string message row: 
        (1581695094.944000) can0 033#fd9f04f600f600
    """

#     # Rationale about the timestamp:
#     print(f"{element['date'] = }")
#     datetime = np.datetime64(element['date'])
#     print(f"{datetime = }")
#     timestamp = (np.datetime64(element['date'], 'ns') -np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')
#     print(f"{timestamp = }")
#     datetime_pandas = pandas.to_datetime(timestamp, unit='s')
#     print(f"{datetime_pandas = }")

    timestamp = (np.datetime64(element['date'], 'ns') 
                 - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')
    timestamp = format(timestamp, "10.6f")

    if debug: print(f"\t\t{element['top'] = }")
    topic = format(int(element['top']), '03x')
#     topic = format(int(str(element['top']), 16), '03')
    if debug: print(f'\t\t{topic = }')

    if debug: print(f"\t\t{element['mod'] = }")
    # Addressing an inconsistency on the dataset generation. For some reason,
    # there are modules with id greater than 255, which is impossible in
    # practice.
    module = format(int(element['mod']), '02x')
#     while len(str(module)) > 2:
#         if debug: print(f'\t\t{module = }')
#         module = format(int(module), '02x')
    if debug: print(f'\t\t{module = }')

    if debug: print(f'\t\t{element["bytes"] = }')
    bytes = ''.join([f'{int(byte):02x}' for byte in element['bytes']])
    if debug: print(f'\t\t{bytes = }')

    payload = module + bytes
    if debug: print(f'\t\t{payload = }')

    candump_message = f"({timestamp}) can0 {topic}#{payload}"

    if (len(payload) != (2 + 2*len(element["bytes"]))) or (len(payload) % 2):
        print()
        print('ERROR!')
        print(f'\t{len(payload) = }')
        print(f'\t{(2 + 2*len(element["bytes"])) = }')
        print()
        print(f'\t{element = }')
        print()
        print(f"\t\t{element['top'] = }")
        print(f'\t\t{topic = }')
        print(f"\t\t{element['mod'] = }")
        print(f'\t\t{module = }')
        print(f'\t\t{module = }')
        print(f'\t\t{element["bytes"] = }')
        print(f'\t\t{bytes = }')
        print(f'\t\t{payload = }')
        print()
        print(f'\t{candump_message = }')
        exit(-1)
        raise()

    return candump_message


def convert_csv_to_json_file(input_filename: str, output_filename):
    # Convert .csv files to .json (as records) files
    for i in [0, 1]:
        print(f'Processing \'{input_filename}\'.')
        df = pd.read_csv(
            input_filename,
            usecols=['date', 'top', 'mod', 'bytes'],
            dtype={'data': str, 'top': pd.Int64Dtype(), 'mod': pd.Int64Dtype()},
        ).dropna()
        df = df.sort_values('date', ignore_index=True)
        df.bytes = df.bytes.apply(json.loads)
        print(df.head())
        df.to_json(output_filename, orient='records', indent=True)
        print(f'Saved to \'{output_filename}\'.')


def convert_json_db_to_candump_log_file(input_filename: str, output_filename: str, debug=False):
    input_file = open(input_filename, 'rb')
    output_file = open(output_filename, 'w+')

    j = list(ijson.items(input_file, ''))[0]

    # Setup progress bar
    pbar = tqdm(total=(len(j) - 1), file=sys.stdout)

    print(f'Processing \'{input_filename}\'.')
    for element in j:
        if len(element['bytes']) > 0:
            if debug: print(f'\t{element = }')

            row = convert_json_db_to_candump_log_row(element)

            if debug: print(f'\t{row = }')

            output_file.write(f'{row}\n')

            pbar.update(1)  # Progress bar update


    print(f'Saved to \'{output_filename}\'.')
    input_file.close()
    output_file.close()


def main():
    for i in [0, 1]:
        csv_file = f'../data/candump_vinicius/db{i}.csv'
        json_file = f'../data/candump_vinicius/db{i}.json'
        log_file = f'../data/candump/candump-from_db{i}.log'

        convert_csv_to_json_file(csv_file, json_file)

        convert_json_db_to_candump_log_file(json_file, log_file)

    print("Done!")

if __name__ == '__main__':
    main()

