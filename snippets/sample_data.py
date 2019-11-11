"""
Sample yelp-data and generate relevant sample sets.
"""

from awstools import s3
import dask
from dask.distributed import Client, progress
import pandas as pd
import dask.dataframe as dd
import os

###Connect to S3###
def get_bucket(bucket_name = 'yelp-data-shared-labs18'):
    return s3.Bucket(bucket_name)


def download_file_from_bucket(bucket_file_path, save_file_path):
    bucket = get_bucket()
    bucket.get(bucket_file_path, save_file_path)


###Create Dask Client###
def create_dask_client(n_workers=2, threads_per_worker=1, memory_limit='8GB'):
    return Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        asynchronous=False,
        local_directory='/tmp/')


###Load JSON into Dask Dataframe###
def load_json(filename, npartitions=4):
    """
    Download json file and load into dask dataframe.

    Parameters
    ----------

    """
    filepath = os.path.join(os.getcwd(), filename)
    try:
        dask_df = dd.from_pandas(pd.read_json(filepath), npartitions)
    except:
        dask_df = dd.from_pandas(pd.read_json(filepath, lines=True), npartitions)
    return dask_df


# Calculate fraction of dataset to capture
def generate_fraction_list(file_list, max_file_size=20):
    return [(x[0], max_file_size/x[1]) for x in file_list]


def sample_data(dataframe, fraction, filename=None):
    """
    Return sampled dataframe or save as parquet file.
    """
    if filename is None:
        return dataframe.sample(frac=fraction, replace=False).compute()
    dataframe.sample(frac=fraction, replace=False).compute().to_parquet(
        filename
    )


def load_and_sample_json(file_info):
    """
    Take in tuple-like object of file information and wanted sample size. Generates sample data.

    Parameters
    ----------
    file_info: tuple-like
        **(filename, fraction_to_retain)

    Returns
    -------
    None
        **Creates and saves filename.parquet to .data/filename.parquet
    """
    root_name = file_info[0].split('.')[0]
    out_path = os.path.join(os.getcwd(), '.data/' + root_name + '.parquet')
    df = load_json(file_info[0])
    sample_data(df, file_info[1], out_path)
    print('Run load/sample function for {}'.format(file_info[0]))


if __name__ == "__main__":
    ###Get/Transcribe File Data###
    file_list = [
        ('business.json', 132),
        ('user.json', 2300),
        ('review.json', 5000),
        ('photo.json', 25),
        ('checkin.json', 390),
        ('tip.json', 234),
    ]


    ###Dask Distributed Workflow###
    # Start Client
    client = create_dask_client()

    # Map function to files
    for file_info in generate_fraction_list(file_list=file_list):
        load_and_sample_json(file_info)

    # Close Dask Client
    client.close()

    ###Save Sampled Data Files###
    # Open Bucket and Write files to sample_data
    bucket = get_bucket()
    for file in file_list:
        root_name = file_info[0].split('.')[0]
        file_path = os.path.join(os.getcwd(), '.data/' + root_name + '.parquet')
        save_name = 'sample_data/' + root_name + '.parquet'
        bucket.save(file_name=file_path, object_name=save_name)
