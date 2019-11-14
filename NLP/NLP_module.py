from awstools.awstools import s3
import pandas as pd
import json


# Main while loop
while is_jobs_empty() == False:
    path = read_next_job()
    download_data(path, 'data_file.json')
    df = get_text_df('data_file.json')
    processed_data = process(df)
    put_in_processed(processed_data)
    delete_last_job()

# Functions
def is_jobs_empty(): #Done
    bucket = s3.Bucket('yelp-data-shared-labs18')
    jobs = []
    jobs = bucket.get_dir_contents('Jobs')
    if jobs == []:
        return True
    else: return False

def read_next_job(): #Done
    bucket = s3.Bucket('yelp-data-shared-labs18')
    jobs = []
    jobs = bucket.get_dir_contents('Jobs')
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
    ###
    ###
    s3.upload_file(processed_file_path, 'Processed', object_name=None) #s3 function, change path to processed

# Done
def delete_last_job():
    bucket = s3.Bucket('yelp-data-shared-labs18')
    jobs = []
    jobs = bucket.get_dir_contents('Jobs')
    path = jobs[0].get('Key')
    bucket.delete_object(path)
