### Very rough first start at comparison module ###
# Module for addding list of nearest_neighbor business ID's in DB
# TODO need to generate jobs first? get data from db directly?

import pandas as pd
import logging
import os
import json
from pandas.io.json import json_normalize
from config import DATABASE_URI
from models import *
from db import get_session
from sqlalchemy import or_
from jobs import get_jobs, pop_current_job, read_job, \
    download_data, delete_local_file, delete_s3_file, load_data, \
        write_data, generate_job
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors

###-------------Logging-------------------###
log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

###---------Processing Functions----------###
def get_business_df(business_id):
    with get_session() as session:

        # Getting info about business for filtering
        business_query = session.query(
            Business.state, Business.latitude, Business.longitude, \
            Business.hours, Business.attributes, Business.categories 
            ).filter(Business.business_id==business_id).all()
        business_df = pd.DataFrame(business_query)
        business_df = clean_business_data(business_df)

    return business_df


def get_filtered_df(business_df):

    state = business_df.iloc[0].state
    categories = business_df.iloc[0].categories

    with get_session() as session:
        filter_query = session.query(Business.latitude, \
            Business.longitude, Business.attributes, Business.categories \
            ).filter(Business.state==state).filter(or_( \
            *[Business.categories.contains(category) for category in \
            categories])).all()
        filtered_df = pd.DataFrame(filter_query)
    
    filtered_df = clean_business_data(filtered_df)

    return filtered_df

def get_categories(categories):
    categories = set(categories.split(', '))
    return categories

def clean_business_data(df):

    # convert attributes from string to dict
    df['attributes'] = df.attributes.apply(json.loads)

    # create new column for each attribute
    attributes = json_normalize(df['attributes'])

    # combining attributes and original dataframes
    cleaned = pd.concat([df, attributes], axis=1)
    cleaned.drop(['attributes'], axis='columns', inplace=True)

    # converting categories from string to set
    cleaned.categories = df.categories.apply(get_categories)

    return cleaned

def extra_categories(categories, business_categories):
    n_common_categories = len(categories & business_categories)
    extra_categories = len(categories) - n_common_categories
    return extra_categories

def category_check(categories, category):
    if category in categories:
        return 1
    else:
        return 0

def get_comparison_group(filtered_df, business_df):
    # creating new column for each category in original business
    business_categories = business_df.categories.iloc[0]
    for category in business_categories:
        filtered_df[category] = filtered_df.categories.apply(category_check, args=[category,])
        business_df[category] = 1
    
    # creating new column with count of categories 
    # the original business didn't have
    business_df['extra_categories'] = 0
    filtered_df['extra_categories'] = filtered_df.categories.apply( \
        extra_categories, args=[business_categories,])

    # create column for number 
    # TODO impute missing values in businesses?
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(filtered_df)
    df_scaled = pd.DataFrame(df_scaled, columns = df.columns)
    
    neigh = NearestNeighbors(n_neighbors=20)
    neigh.fit(df_scaled)
    # business must be in same form as df
    business = get_row_from_id(business_id)
    distances, indices = neigh.kneighbors(business)
    comps = []
    for i in range(10):
        comp_index = indices[0,i]
        comps.append(df.iloc[comp_index].player)
    return comps

def add_comparisons(df):
    df['comparisons'] = df.business_id.apply(get_comparison_group)
    pass


### TODO Change to read all business_ids from DB, not jobs/S3 ###
if __name__ == "__main__":

    main_logger = logging.getLogger(__name__+" Comparisons Adder")

    num_jobs = len(get_jobs('comparisons')) # No module creates comparison jobs.  Manually create these.

    for i in range(num_jobs):
        # Get a job and read out the datapath
        current_job = pop_current_job()
        asset = read_job(current_job).get('file')

        main_logger.info('Running job {}.  Read file {}'.format(current_job, asset))

        # Load the data
        datapath = download_data(asset)
        df = load_data(datapath)
        business_df = get_business_df(business_id)
        filtered_df = get_filtered_df(business_df)
        # sentiment_df = add_comparisons(df)

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
    # read job
    # get appropriate file
    # for each business_id:
    #   businesses = filtered_data(business_id)
    #   comps = get_comparison_group
    pass

