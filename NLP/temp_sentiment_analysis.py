"""
Temporary sentiment adder for reference

Goal:  Create textblob objects from text in the text column of a dataframe
        Subset df with review_id and sentiment columns.
        Save data to S3.
        Generate POST jobs for saved data
"""

## need to generate jobs first

import logging
import os

# installing / importing textblob
os.system('pip install textblob')
from textblob import TextBlob

from jobs import get_jobs, pop_current_job, read_job, \
    download_data, delete_local_file, delete_s3_file, load_data, \
        write_data, generate_job
    
###Logging###
log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

### Processing functions ###
def process_text(text):
    blob = TextBlob(text)

    sentiment = blob.sentiment
    """sentiment is a tuple
        
        sentiment[0] = polarity 
        range: -1 (most negative) to 1 (most positive)

        sentiment[1] = objectivity
        range: 0 (most subjective) to 1 (most objective)
    """
    return sentiment

def get_polarity(tuple):
    return tuple[0]

def get_subjectivity(tuple):
    return tuple[1]

def add_sentiment(df):
    df['sentiment'] = df.text.apply(process_text)
    df['polarity'] = df.sentiment.apply(get_polarity)
    df['subjectivity'] = df.sentiment.apply(get_subjectivity)
    df = df.filter(['review_id', 'tip_id', 'polarity', 'subjectivity']) 
    return df

if __name__ == "__main__":
    main_logger = logging.getLogger(__name__+" Sentiment Adder")

    num_jobs = len(get_jobs('sentiment')) # No module creates sentiment jobs.  Manually create these.

    for i in range(num_jobs):
        # Get a job and read out the datapath
        current_job = pop_current_job()
        asset = read_job(current_job).get('file')

        main_logger.info('Running job {}.  Read file {}'.format(current_job, asset))

        # Load the data
        datapath = download_data(asset)
        data = load_data(datapath)
        sentiment_df = add_sentiment(data)

        # Write Data to s3
        savepath = asset+'_sentiment'
        write_data(data=sentiment_df, savepath=savepath, dry_run=False)
        
        # Generate POST Job
        review = asset.split('_')[1] == 'review'
        if review:
            generate_job(savepath, 'POST', tablename='review_sentiment', dry_run=False)
        else:
            generate_job(savepath, 'POST', tablename='tip_sentiment', dry_run=False)

        # Cleanup
        delete_local_file(datapath)
        delete_s3_file(current_job)
        main_logger.info("Deleted Job: {}".format(current_job))