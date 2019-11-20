"""
Temporary Tokenization Fixer for Reference.

Goal:  Scan tokens in the token column of a dataframe
        Create a subset df with review_id and tokens.
        Save data to S3.
        Generate POST jobs for saved data
"""

import logging
import os
import pandas as pd
import dask.dataframe as dd
import time


# downloading spacy dependencies
os.system('pip install spacy')
import spacy
os.system('python -m spacy download en_core_web_lg')
nlp = spacy.load('en_core_web_lg') 

from jobs import get_jobs, pop_current_job, read_job, get_bucket, \
     download_data, delete_local_file, delete_s3_file, load_data, \
         write_data, generate_job


#############
###Logging###
#############

log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)


### Processing functions ###
def process_text(text):
    doc = nlp(text)

    # Defining parts of speech to keep in tokens and lemmas
    POS = ['ADJ', 'NOUN', 'PROPN', 'VERB', 'ADV', 'INTJ']

    # Getting lemmas and tokens
    lemmas = []
    tokens = []
    for token in doc:
        if ((token.is_stop != True) 
        and (token.is_punct != True) 
        and (token.pos_ in POS)):
            tokens.append(token.text)
            lemmas.append(token.lemma_)
    return (tokens, lemmas)

######################
###Helper Functions###
######################
  
def get_tokens(tuple):
    return tuple[0]

def get_lemmas(tuple):
    return tuple[1]

def filter_tokens(df):
    df['tuple'] = df.text.apply(process_text)
    df['tokens'] = df.tuple.apply(get_tokens)
    df['lemmas'] = df.tuple.apply(get_lemmas)
    df = df.filter(['review_id', 'tokens', 'lemmas']) 
    return df

if __name__ == "__main__":
    main_logger = logging.getLogger(__name__+" Token Fixer")

    num_jobs = len(get_jobs('retoken')) # No module creates retoken jobs.  Manually create these.

    bucket = get_bucket()

    for i in range(num_jobs):
        # Get a job and read out the datapath
        get_jobs(job_type='retoken')
        current_job = pop_current_job()
        asset = read_job(current_job)['File']

        main_logger.info('Running job {}.  Read file {}'.format(current_job, asset))

        # Load the data
        datapath = download_data(asset)
        data = load_data(datapath)
        filtered = filter_tokens(data)

        # DASK: Partition Data
        num_vcpu = 6
        daskdf = dd.from_pandas(data, npartitions=num_vcpu)
        ###INSERT TOKEN CLEANING FUNCTION HERE###
        start = time.time()
        # DASK: Map function to data
        result = daskdf.map_partitions(clean_tokens, meta=data)

        # DASK: Execute Compute - get Pandas dataframe back
        output = result.compute()
        stop = time.time()
        main_logger.info("{} processed in {}".format(len(data), stop-start))

        # Write Data to s3
        savepath = asset.split('/')[-1].split('.')[0] + '_retoken'
        write_data(data=output, savepath=savepath)

        # Generate POST Job
        generate_job(savepath, 'POST')

        # Cleanup
        delete_local_file(datapath)
        delete_s3_file(current_job)
        main_logger.info("Deleted Job: {}".format(current_job))