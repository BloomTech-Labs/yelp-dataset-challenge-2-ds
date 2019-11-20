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

from jobs import get_jobs, pop_current_job, read_job, get_bucket, \
     download_data, delete_local_file, delete_s3_file, load_data, \
         write_data, generate_job

#############
###Logging###
#############

log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

######################
###Helper Functions###
######################



###INSERT CLEANING/PROCESSING FUNCTION HERE###


if __name__ == "__main__":
    main_logger = logging.getLogger(__name__+" Token Fixer")

    num_jobs = len(get_jobs('retoken')) # No module creates retoken jobs.  Manually create these.

    bucket = get_bucket()

    for i in range(num_jobs):
        # Get a job and read out the datapath
        current_job = pop_current_job()
        asset = read_job(current_job)['File']

        main_logger.info('Running job {}.  Read file {}'.format(current_job, asset))

        # Load the data
        datapath = download_data(asset)
        data = load_data(datapath)

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
        savepath = asset.split('/')[-1].split('.')[0] + '_retokenize'
        write_data(data=output, savepath=savepath)

        # Generate POST Job
        generate_job(savepath, 'POST')

        # Cleanup
        delete_local_file(datapath)
        delete_s3_file(current_job)
        main_logger.info("Deleted Job: {}".format(current_job))
