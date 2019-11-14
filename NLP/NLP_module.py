# !pip install pyarrow
from awstools.awstools import s3
import pandas as pd
import json

# Need to already have access to s3 bucket
# Run the line below with the keys to get access 
# s3.setup_aws(key_id=[ACCESS_KEY_ID], secret_key=[SECRET_ACCESS_KEY])

# Only reads .parquet files


class job_list():
    def __init__(self, job_list=None):
        self.job_list = job_list

new_jobs = job_list()
bucket = s3.Bucket('yelp-data-shared-labs18')

# Main while loop
while is_nlp_jobs_empty(bucket) == False:
    path = read_next_job(bucket)
    df = get_df(path)
    processed_df = process(df)
    put_in_processed(processed_df, path)
    delete_last_job(bucket)

# Functions
def is_nlp_jobs_empty(bucket): #Done
    return len(get_nlp_jobs(bucket)) == 0

def get_nlp_jobs(bucket):
    if new_jobs.job_list == None:
        jobs = []
        nlp_jobs = []
        jobs = bucket.get_dir_contents('Jobs')
        for job in jobs:
            if 'NLP' or 'nlp' in job:
                nlp_jobs.append(job)
        new_jobs.job_list = nlp_jobs
    else:
        return new_jobs.job_list

def read_next_job(): #Done
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    download_data(path, 'jobs_task.json')

    with open('jobs_task.json') as job_file:
        job = json.load(job_file)
        next_job = job.get('file')

    return next_job

# Done, only needed for json files, parquet files are not downloaded locally
def download_data(path, save_name=None):
    s3.download_file('yelp-data-shared-labs18', path, save_name=save_name)

# Done
def get_df(data_file):
    
    # This code works to convert json files but we are not currently planning to do that
    # Leaving it here in case we want to expand functionality later, but that would require changes elsewhere
    # if data_file[-4:] == 'json':
    #     data = []
    #     for line in open(data_file, 'r'):
    #         row = json.loads(line)
    #         data.append(row)
    #     df = pd.DataFrame(data)
    #     return df
    
    if data_file[-7:] == 'parquet':
        data = s3.download_file('yelp-data-shared-labs18', data_file)
        df = pd.read_parquet(data)
        return df
    
    raise TypeError('Invalid file type, must be a .parquet file')    

def process(df):
    # Aki's NLP function
    pass

# Done
def put_in_processed(df, path):
    # getting the original file name from the path
    filename = path.split('/')[-1]

    # creating temporary local parquet file
    df.to_parquet('temp_parquet_file.parquet')

    processed_file_path = 'Processed/' + filename
    s3.upload_file(file_path='temp_parquet_file.parquet', bucket='yelp-data-shared-labs18', object_name=processed_file_path)

# Done
def delete_last_job(bucket):
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    bucket.delete_object(path)
    if len(jobs) == 1:
        new_jobs.job_list = None
    else:
        new_jobs.job_list = jobs[1:]