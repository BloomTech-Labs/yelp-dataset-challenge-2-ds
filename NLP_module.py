# !pip install pyarrow
from awstools.awstools import s3
import pandas as pd
import json
import spacy

# Need to download en_core_web_lg-2.2.0
nlp = spacy.load("en_core_web_lg-2.2.0")
print('loaded spacy corpus')

# Need to already have access to s3 bucket
# Run the line below with the keys to get access 
# s3.setup_aws(key_id=[ACCESS_KEY_ID], secret_key=[SECRET_ACCESS_KEY])

# Only reads .parquet files


class job_list():
    def __init__(self, job_list=None):
        self.job_list = job_list

new_jobs = job_list()
bucket = s3.Bucket('yelp-data-shared-labs18')
print('connected to bucket')

# Functions
def is_nlp_jobs_empty(bucket): #Done
    return len(get_nlp_jobs(bucket)) == 0

def get_nlp_jobs(bucket):
    if new_jobs.job_list == None:
        jobs = []
        nlp_jobs = []
        ##  
        ##  TODO - Change from test_jobs back to jobs
        ##
        jobs = bucket.get_dir_contents('Test_jobs')
        for job in jobs:
            if 'nlp' in job.get('Key').lower(): # case insensitive
                nlp_jobs.append(job)
        new_jobs.job_list = nlp_jobs
        return new_jobs.job_list
    else:
        return new_jobs.job_list

def read_next_job(bucket): #Done
    jobs = get_nlp_jobs(bucket)
    path = jobs[0].get('Key')
    download_data(path, 'jobs_task.json')

    with open('jobs_task.json') as job_file:
        job = json.load(job_file)
        next_job = job.get('file')

    print('working on file: ' + next_job)
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
    # df['lemmas'], df['noun_chunks'], df['vectors'] = df['text'].apply(process_text)
    pass

# def process_text(text):
#     doc = nlp(text)

#     # Getting lemmas
#     lemmas = []
#     for token in doc:
#         if (token.is_stop != True) and (token.is_punct != True):
#             lemmas.append(token.lemma_)
    
#     # Getting noun_chunks
#     noun_chunks = list(doc.noun_chunks)

#     # Getting vectors
#     vector = doc.vector

#     return lemmas, noun_chunks, vector

# def process_df(df):
#     doc = nlp(df.text)

#     # Getting lemmas
#     lemmas = []
#     for token in doc:
#         if (token.is_stop != True) and (token.is_punct != True):
#             lemmas.append(token.lemma_)
#     df['lemmas'] = lemmas

#     # Getting noun_chunks
#     noun_chunks = list(doc.noun_chunks)
#     df['noun_chunks'] = noun_chunks
#     # Getting vectors
#     vector = doc.vector
#     df['vectors'] = vector

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

# Main while loop
while is_nlp_jobs_empty(bucket) == False:
    path = read_next_job(bucket)
    df = get_df(path)
    # processed_df = process(df) TODO uncomment
    put_in_processed(df, path) # TODO change to processed_df instead of df
    delete_last_job(bucket)

