from awstools.awstools import s3
import pandas as pd
import json

class job_list():
    def __init__(self, job_list=None):
        self.job_list = job_list

new_jobs = job_list()
bucket = s3.Bucket('yelp-data-shared-labs18')

# Main while loop
while is_nlp_jobs_empty(bucket) == False:
    path = read_next_job(bucket)
    download_data(path, 'data_file.json')
    df = get_text_df('data_file.json')
    processed_data = process(df)
    put_in_processed(processed_data)
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
            if 'nlp' in job:
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

# Done
def download_data(path, save_name=None):
    s3.download_file('yelp-data-shared-labs18', path, save_name=save_name)

# Done, Only works with json data files
##
##
## handle parquet files df.read_parquet(bucket.get...)
def get_text_df(data_file):
    data = []
    for line in open(data_file, 'r'):
        row = json.loads(line)
        data.append(row)

    df = pd.DataFrame(data)

    return df

def process(df):
    # Aki's NLP function
    pass

def put_in_processed(df):
    # Need to create file with processed data
    

    # save as parquet file
    ###

    ###
    # processed_file_path = 'Processed/' + filename
    # s3.upload_file(file_path=, 'yelp-data-shared-labs18', object_name=processed_file_path) #s3 function, change path to processed
    pass
# Done
def delete_last_job(bucket):
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    bucket.delete_object(path)
    if len(jobs) == 1:
        new_jobs.job_list = None
    else:
        new_jobs.job_list = jobs[1:]