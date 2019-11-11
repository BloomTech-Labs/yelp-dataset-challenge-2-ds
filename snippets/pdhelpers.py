"""
Pandas Helpers

Some helper functions for Pandas working with our data.
"""

def df_to_query(df, tablename):
    """Transform dataframe into dictionary object of correct form for database api request parsing.

    param df: Tabular data to transform
    type df: Pandas.DataFrame
    param tablename: Name of table to store data in.  Must match existing table, case.
    type tablename: string
    """
    import json

    def transform_df(df):
        # Generate a list of stringified dictionaries from the dataframe
        #   Note: Will transform the entire supplied dataframe.  Split datframe into chunks prior.
        records_list = df.to_json(orient='records', lines=True).split('\n')
        # Cast stringifield row entries as python dict via json loads (important for request)
        cast_records = [json.loads(x) for x in records_list]
        return cast_records

    package = {
        'table_name': tablename,
        'data': transform_df(df)
    }

    return package


def generate_id(record):
    """Generate ID returns a repeatable hash of a given record.

    param record: python string, list, or dictionary
    type record: string
    """
    import hashlib
    import json
    import pandas as pd
    # Convert series to dictionary object for encoding
    if type(record) == pd.Series:
        record = record.to_dict()
    # Encode record to bytes
    record = record.encode()
    return hashlib.sha256(json.dumps(record)).hexdigest()


def build_databunch(query, num_splits=3, max_size=None):
    import math
    databunch = []

    # Caclulate number or splits or set (dependent on max_size)
    if max_size:
        num_splits = math.ceil(len(query['data'])/max_size)

    bunch_size = int(len(query['data']) / num_splits)

    for i in range(num_splits):
        if i < num_splits-1:
            data_range = (i*bunch_size, (i+1)*bunch_size)
        else:
            data_range = (i*bunch_size, len(query['data']))
        databunch.append(
            {
                'table_name': query['table_name'],
                'data': query['data'][data_range[0]:data_range[1]]
            }
        )
    return databunch