# !pip install pyarrow
import s3
import NLP_processing
import pandas as pd
import json
import logging
import os

###Logging###
main_logger = logging.getLogger(__name__+" NLP Controller")
log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

# Need to already have access to s3 bucket
# Run the line below with the keys to get access
# s3.setup_aws(key_id=[ACCESS_KEY_ID], secret_key=[SECRET_ACCESS_KEY])

# Only reads .parquet files

class job_list():
    def __init__(self, job_list=None):
        self.job_list = job_list

# Functions
def is_nlp_jobs_empty(bucket):
    return len(get_nlp_jobs(bucket)) == 0

def get_nlp_jobs(bucket):
    if new_jobs.job_list == None:
        jobs = []
        nlp_jobs = []
        jobs = bucket.get_dir_contents('Jobs')
        for job in jobs:
            if 'nlp' in job.get('Key').lower(): # case insensitive
                nlp_jobs.append(job)
        new_jobs.job_list = nlp_jobs
        return new_jobs.job_list
    else:
        return new_jobs.job_list

def read_next_job(bucket):
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    main_logger.info("Starting job {}".format(path.split('/')[-1]))
    download_data(path, 'jobs_task.json')

    with open('jobs_task.json') as job_file:
        job = json.load(job_file)
        next_job = job.get('Key')

    print('working on file: ' + next_job)
    return next_job

# Only works for json files
def download_data(path, save_name=None):
    s3.download_file('yelp-data-shared-labs18', path, save_name=save_name)

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

    data = s3.download_file('yelp-data-shared-labs18', data_file)
    df = pd.read_parquet(data)
    return df

def process(df):
    processed_df = NLP_processing.process_df(df)
    return processed_df

def put_in_processed(df, path):
    # getting the original file name from the path
    filename = path.split('/')[-1]
    # creating temporary local parquet file
    df.to_parquet('temp_parquet_file.parquet')
    processed_file_path = 'Processed/' + filename
    s3.upload_file(file_path='temp_parquet_file.parquet', bucket='yelp-data-shared-labs18', object_name=processed_file_path)
    generate_job(savepath=processed_file_path, job_type="POST")

def delete_last_job(bucket):
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    bucket.delete_object(path)
    if len(jobs) == 1:
        new_jobs.job_list = None
    else:
        new_jobs.job_list = jobs[1:]

def generate_job(savepath, job_type):
    job_data = {
        'Key': savepath
    }
    job_name = ''.join([job_type, '_', savepath.split('/')[-1], '_job.json'])
    temp_job_path = '/tmp/'+job_name
    with open(temp_job_path, 'w') as file:
        json.dump(job_data, file)
    bucket.save(temp_job_path, 'Jobs/{}'.format(job_name))
    os.remove(temp_job_path)

if __name__ == "__main__":

    new_jobs = job_list()
    bucket = s3.Bucket('yelp-data-shared-labs18')
    print('connected to bucket')

    # Main while loop
    while is_nlp_jobs_empty(bucket) == False:
        path = read_next_job(bucket)
        df = get_df(path)
        processed_df = process(df)
        put_in_processed(processed_df, path)
        delete_last_job(bucket)
        break  # Remove break to run all jobs.  For Testing/Timing Purposes only.


