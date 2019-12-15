"""
Test file with example usage for awstools.  Built for yelp data challenge.

Includes tests for jobs

"""
import os
import jobs
import unittest
import warnings

###############
### s3 Tests###
###############

## Current version may require .aws folder and credential file be created.
    ## Setup AWS credentials and create local credential files.
# s3.setup_aws()

class TestJobs(unittest.TestCase):

    def setUp(self):
        # Create local job for upload
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        jobs.generate_job('test', 'test', dry_run=False)  # Tests generate_job to some extent
        self.jobs = jobs.get_jobs(job_type='test')

    def test_get_bucket(self):
        # Check that a bucket object is returned via context manager and it is available globally.
        jobs.get_bucket()
        self.assertIsNotNone(jobs.g.bucket)

    def test_generate_job(self):
        # Check that save paths and job data match inputs (dry-run test)
        # generate jobs returns (temp_job_path, job_name, job_data)
        objectpath = 'test_object_path'
        job_type = 'TEST'
        tablename = 'test_table'
        job_name = ''.join([job_type, '_', objectpath.split('/')[-1], '_job.json'])
        temp_job_path = '/tmp/'+job_name

        job_data = {
        'file': objectpath,
        'tablename': tablename,
        'test_kwarg': "test_data_value",
            }

        job_return = jobs.generate_job(
            objectpath='test_object_path',
            job_type='TEST',
            tablename='test_table',
            test_kwarg="test_data_value",
             )

        self.assertListEqual(list((temp_job_path, job_name, job_data)), list(job_return))

    # def test_read_job(self):
    #     # Check contents returns non-null (bucket selected must not be empty)
    #     raise NotImplementedError

    # def test_valid_job(self):
    #     # Check tablename exists in database model.  Check critical keys are produced.
    #     raise NotImplementedError



if __name__ == '__main__':
    unittest.main()
