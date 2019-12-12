### Very rough first start at comparison module ###
# Module for addding list of nearest_neighbor business ID's in DB
# TODO need to generate jobs first? get data from db directly?

import pandas as pd
import logging
import os
from jobs import get_jobs, pop_current_job, read_job, \
    download_data, delete_local_file, delete_s3_file, load_data, \
        write_data, generate_job

from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors

###-------------Logging-------------------###
log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

###---------Processing Functions----------###
def filtered_data(business_id):
    # connect to db
    # filter by business state and category
    # return df of relevant businesses
    pass

def get_row_from_id(business_id):
    pass

def get_comparison_group(business_id):
    businesses = filtered_data(business_id)
    # TODO impute missing values in businesses?
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(businesses)
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
        sentiment_df = add_comparisons(df)

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

