"""Write on Job processes POST jobs in Jobs/
    Load data in Job, Convert to databunch, package, and sent POST.
"""
import requests
import numpy as np
import pandas as pd
import time
from multiprocessing import Pool
from functools import partial
import logging
import os
from jobs import get_jobs, pop_current_job, read_job,\
     download_data, delete_local_file, delete_s3_file, load_data


###Logging###
request_logger = logging.getLogger(__name__+" requests")
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
    p.close()
    p.join()


def run_request(bunch, url, retry_size=20):
    """Run and time a request with the python requests library
    """
    import requests
    import time
    import numpy as np
    try:
        time.sleep(np.random.random_sample()*10)
        start = time.time()
        response = requests.post(url=url, json=bunch, timeout=None)
        assert response.status_code == 200
        request_logger.info("POST succeded.  Status= {}".format(response.status_code))
        stop = time.time()
        request_logger.info('Batch of {} processed in {}'.format(len(bunch['data']), stop-start))
        return True
    except:
        min_size = retry_size - 1
        request_logger.error("POST failed.  Trying again with smaller bunch of {}.".format(min_size))
        if min_size < 1:
            request_logger.error("POST failed at single element.  Dropping Request.")
            return False
        databunch = build_databunch(query=bunch, max_size=min_size)
        for mini_bunch in databunch:
            run_request(bunch=mini_bunch, url=url, retry_size=min_size)

# Deprecated. Table name now in job file under key tablename
# def get_source_from_name(filename):
#     for table_name in tables.keys():
#         if table_name in filename:
#             return tables[table_name]
#     raise NameError('Tablename not found.  Aborting.')


# tables = {
#     'business': 'businesses',
#     'user': 'users',
#     'checkin': 'checkins',
#     'photo': 'photos',
#     'tip': 'tips',
#     'review': 'reviews',
# }




if __name__ == "__main__":
    write_logger = logging.getLogger(__name__+' DB-writer')

    num_jobs = len(get_jobs('post'))
    for i in range(num_jobs):
        # Get a job and read out the datapath
        current_job = pop_current_job()
        asset = read_job(current_job)['file']
        tablename = read_job(current_job)['tablename']
        write_logger.info('Running job {}.  Read file {}'.format(current_job, asset))

        # Load the data
        datapath = download_data(asset)
        data = load_data(datapath)

        # Build query package
        package = df_to_query(df=data, tablename=tablename)

        # Split package
        databunch = build_databunch(query=package, max_size=50)

        # Connect and write to database via api
        parallel_post_requests(
            databunch=databunch,
            url='https://db-api-yelp18-staging.herokuapp.com/api/data',
            max_requests=12
            )

        # Cleanup
        delete_local_file(datapath)
        delete_s3_file(current_job)
        write_logger.info("Deleted Job: {}".format(current_job))
