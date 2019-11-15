"""Write on Job processes POST jobs in Jobs/
    Load data in Job, Convert to databunch, package, and sent POST.
"""
import requests
import numpy as np
import hashlib
import json
import pandas as pd
import time
from multiprocessing import Pool
from functools import partial
import logging
import os
request_logger = logging.getLogger(__name__+" request:")
log_path = os.path.join(os.getcwd(), 'logs/debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)


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


def parallel_post_requests(databunch, url, max_requests=10):
    """Request handler that will parallelize databunch POST requests.

    param databunch: Packages to POST to database API
    type databunch: list of packages
    param max_requests: How many simultaneous requests sessions to attempt
    type max_requests: int
    param url: Endpoint url.  Must be valid ipv4 or dns entry.
    type url: string
    """
    runner = partial(run_request, url=url)
    p = Pool(max_requests)
    p.map(runner, databunch)


def run_request(bunch, url):
    """Run and time a request with the python requests library
    """
    import requests
    import time
    import numpy as np
    try:
        time.sleep(np.random.random_sample()*10)
        start = time.time()
        response = requests.post(url=url, json=bunch)
        assert response.status_code == 200
        request_logger.info("POST succeded.  Status= {}".format(response.status_code))
        stop = time.time()
        request_logger.info('Batch of {} processed in {}'.format(len(bunch['data']), stop-start))
        return True
    except:
        request_logger.error("POST failed.  Trying again")
        run_request(bunch=bunch, url=url)