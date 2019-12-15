"""
Jobs Library
    Read in jobs, Fetch Job Assets, Delete Jobs
"""
import s3
import json
import os
import pandas as pd
from io import BytesIO
import logging

# Logging - no config here.  Relies on parent to configure log location & handler
job_logger = logging.getLogger(__name__)


class g():
    """g, Global Class
            Used to store repeatedly accessed information in the jobs module
    """
    def __init__(self, bucket=None, job_list=None):
        self.bucket=bucket
        self.job_list = job_list

g = g()


##########################
###s3 Control Functions###
##########################

def get_bucket(bucket_name='yelp-data-shared-labs18'):
    if g.bucket == None:
        bucket = s3.Bucket(bucket_name)
        g.bucket = bucket
        return g.bucket
    return g.bucket


def download_data(object_path, save_path='/tmp/'):
    bucket = get_bucket()
    save_path = save_path + object_path.split('/')[-1]
    job_logger.info("Downloading {} to {}".format(object_path, save_path))
    bucket.get(object_path, save_path)
    return save_path


def write_data(data, savepath, dry_run=True, filetype='parquet'):
    print('Saving {}'.format(savepath))
    if dry_run:
        print('Executing Dry Run to {}'.format(savepath))
        file_stream = BytesIO()
        data.to_parquet(file_stream)
        print(pd.read_parquet(file_stream).head())
    else:
        print('Commencing upload of {} to S3'.format(savepath))
        job_logger.info("Uploading {} to S3".format(savepath))
        tempfilename = '/tmp/'+savepath.split('/')[-1]
        if filetype == 'parquet':
            data.to_parquet(tempfilename)
        elif filetype == 'json':
            data.to_json(tempfilename, orient='records')
        else:
            job_logger.error("Only parquet or json saving supported")
            raise TypeError("Only parquet or json saving supported")
        bucket = get_bucket()
        bucket.save(tempfilename, savepath)
        os.remove(tempfilename)

###########################
###Job Control Functions###
###########################


def get_jobs(job_type='post'):
    bucket = get_bucket()
    if g.job_list == None:
        jobs = []
        for job in bucket.find('Jobs/'):
            if job_type in job.lower():
                jobs.append(job)
        g.job_list = jobs
        return g.job_list
    return g.job_list


def pop_current_job():
    """Pop job from list"""
    jobs = get_jobs()
    job_name = jobs.pop()
    return job_name


def read_job(job):
    temp = download_data(job)
    print(temp)
    with open(temp) as jobfile:
        response = json.load(jobfile)
    delete_local_file(temp)
    return response


def generate_job(objectpath, job_type, tablename='', dry_run=True, verbose=True, **kwargs):
    """Generate Job
            Creates json object with necessary naming/format for interapplication
            messaging in the Yelp Dataset Challenge project.

        param objectpath: Location of file for job to act on.
        type objectpath: str
        param job_type: Append a jobtype to filename for search by backend modules
        type job_type: str
        param table_name: name of table to route job_type to (generally POST)
        type table_name: str
        param dry_run: Option to print expected output instead of creating and uploading job
        type dry_run: bool (True/False)
        param **kwargs: Extra information to be included in job in the form param: value.
        type kwargs: str
    """
    bucket = get_bucket()
    job_data = {
        'file': objectpath,
        'tablename': tablename,
    }
    if kwargs:
        job_data = dict(job_data, **kwargs) # Append keyword arguments as keys in job

    job_name = ''.join([job_type, '_', objectpath.split('/')[-1], '_job.json'])
    temp_job_path = '/tmp/'+job_name

    if dry_run:
        if verbose:
            print('Dry Run: Saving {} to {}'.format(temp_job_path, job_name))
        return (temp_job_path, job_name, job_data)
    else:
        with open(temp_job_path, 'w') as file:
            json.dump(job_data, file)
        bucket.save(temp_job_path, 'Jobs/{}'.format(job_name))
        os.remove(temp_job_path)


def delete_s3_file(objectpath):
    bucket = get_bucket()
    return bucket.delete(objectpath)


def delete_local_file(filepath):
    os.remove(filepath)


####################
###Load/Transform###
####################

def load_data(filename):
    """Load Data
        Loads data into Pandas DataFrame

        param filename: full filepath + filename or s3 bucket object name.
        type filename: string
        returns: DataFrame
    """
    filetype = filename.split('.')[-1]
    print('Detected {} file.'.format(filetype))

    if filetype == 'json':
        data = pd.read_json(filename)
    else:  # Default to parquet type
        data = pd.read_parquet(filename)
    return data


if __name__ == "__main__":
    for i in range(2):
        current_job = pop_current_job()
        asset = read_job(current_job)['Key']
