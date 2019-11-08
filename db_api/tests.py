import requests

## Test Helper Functions (from snippets - may be old.  See snippets for up-to-date functions.)

def generate_id(record):
    """Generate ID returns a repeatable hash of a given record.

    param record: python string, list, or dictionary, pandas.series
    type record: string
    """
    import hashlib
    import pandas as pd
    # Convert series to dictionary object for encoding
    if type(record) == pd.Series:
        record = str(record.to_dict())
    else:
        record = str(record)
    # Encode record to bytes
    record = record.encode()
    return hashlib.sha256(record).hexdigest()

def df_to_query(df, tablename):
    """Transform dataframe into dictionary object of correct form for database api request parsing.

    param df: Tabular data to transform
    type df: Pandas.DataFrame
    """
    import json

    def transform_df(df):
        # Generate a list of stringified dictionaries from the dataframe
        #   Note: Will transform the entire supplied dataframe.  Split datframe into chunks prior.
        records_list = df.to_json(orient='records', lines=True).split('\n')
        # Cast stringified row entris as python dict vis json loads (important for request)
        cast_records = [json.loads(x) for x in records_list]
        return cast_records

    package = {
        'table_name': tablename,
        'data': transform_df(df)
    }

    return package

###########
###Tests###
###########

# TEST 1: Simple loading of business with manual dict
test_data = {
    'table_name': 'businesses',
    'data': [
            {
        "business_id": 'pioharegh04q3ur0qha089h23r2q3oiqhbef09q1234h',
        "name": 'Big Biz Inc',
        "latitude": 1.001,
        "longitude": 1.002,
        "postalcode": 1234,
        "numreviews": 9,
        "stars": 3.4,
        "isopen": 0,
        "attributes": 'some number of attributes, maybe a comma',
        "categories": 'some number of categories, maybe a comma',
        },
        {
        "business_id": 'pioharadfq342ha089h23r2q3oiqhbef09q1234h',
        "name": 'Big Biz Competitor Inc',
        "latitude": 1.004,
        "longitude": 1.006,
        "postalcode": 9999,
        "numreviews": 2,
        "stars": 3.8,
        "isopen": 1,
        "attributes": 'some number of attributes, maybe a comma',
        "categories": 'some number of categories, maybe a comma',
        }

    ]
}

## Build post request
# request = requests.post(url='http://localhost:5000/api/data/', json=test_data)
# try:
#     print(request)
# except:
#     print('Test 1 Failed')
#     raise

## TEST 2: Load sample_users.json and attempt time writing to db.
import json
import pandas as pd
import time

# Users
# df = pd.read_parquet('sample_users.parquet')
# package = df_to_query(df=df.head(100), tablename='users')
# batch_size = len(package['data'])

# start = time.time()
# request2 = requests.post(url='http://localhost:5000/api/data/', json=package)
# print(request2)
# stop = time.time()
# print('Batch of {} processed in {}'.format(batch_size, stop-start))

# Tips
df = pd.read_parquet('sample_tips.parquet')
df['tip_id'] = df.apply(generate_id, axis=1)
package = df_to_query(df=df.head(2), tablename='tips')
print(package)
batch_size = len(package['data'])

start = time.time()
request2 = requests.post(url='http://localhost:5000/api/data/', json=package)
print(request2)
stop = time.time()
print('Batch of {} processed in {}'.format(batch_size, stop-start))

# # Reviews
# df = pd.read_parquet('sample_reviews.parquet')
# package = df_to_query(df=df.head(100), tablename='reviews')
# batch_size = len(package['data'])

# start = time.time()
# request2 = requests.post(url='http://localhost:5000/api/data/', json=package)
# print(request2)
# stop = time.time()
# print('Batch of {} processed in {}'.format(batch_size, stop-start))

# # Checkins
# df = pd.read_parquet('sample_checkins.parquet')
# package = df_to_query(df=df.head(100), tablename='checkins')
# batch_size = len(package['data'])

# start = time.time()
# request2 = requests.post(url='http://localhost:5000/api/data/', json=package)
# print(request2)
# stop = time.time()
# print('Batch of {} processed in {}'.format(batch_size, stop-start))

# # Photos
# df = pd.read_parquet('sample_photos.parquet')
# package = df_to_query(df=df.head(100), tablename='photos')
# batch_size = len(package['data'])

# start = time.time()
# request2 = requests.post(url='http://localhost:5000/api/data/', json=package)
# print(request2)
# stop = time.time()
# print('Batch of {} processed in {}'.format(batch_size, stop-start))