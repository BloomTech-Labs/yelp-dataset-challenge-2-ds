"""
Jobs Library
    Read in jobs, Fetch Job Assets, Delete Jobs
"""
from awstools import s3
import json

class g():
    def __init__(self, bucket=None, job_list=None):
        self.bucket=bucket
        self.job_list = job_list

g = g()


##########################
###s3 Control Functions###
##########################

def get_bucket():
    if g.bucket == None:
        bucket = s3.Bucket('yelp-data-shared-labs18')
        g.bucket = bucket
        return g.bucket
    return g.bucket

def download_data(object_path, save_path=''):
    bucket = get_bucket()
    save_path = save_path + object_path
    bucket.get(object_path, save_path)
    return save_path


###########################
###Job Control Functions###
###########################


def get_jobs(job_type='post'):
    bucket = get_bucket()
    if g.job_list == None:
        jobs = []
        for job in bucket.find('Jobs/', suffix='json'):
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
    return json.loads(download_data(job))


def delete_job_file(job_filepath):
    bucket = get_bucket()
    return bucket.delete_object(job_filepath)


if __name__ == "__main__":
    for i in range(2):
        current_job = pop_current_job()
        download_data(current_job)