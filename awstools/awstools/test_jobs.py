"""
Test file with example usage for awstools.  Built for yelp data challenge.

Includes tests for jobs

"""
import os
import jobs
import unittest

###############
### s3 Tests###
###############

## Current version may require .aws folder and credential file be created.
    ## Setup AWS credentials and create local credential files.
# s3.setup_aws()

class TestJobs(unittest.TestCase):

    def setUp(self):
        # Create local job for upload
        jobs.generate_job('test', 'test', dry_run=False)  # Tests generate_job to some extent
        self.jobs = jobs.get_jobs(job_type='test')

    def test_get_bucket(self):
        # Check that a bucket object is returned via context manager and it is available globally.
        jobs.get_bucket()
        self.assertIsNotNone(jobs.g.bucket)

    # def test_read_job(self):
    #     # Check contents returns non-null (bucket selected must not be empty)
    #     raise NotImplementedError

    # def test_valid_job(self):
    #     # Check tablename exists in database model.  Check critical keys are produced.
    #     raise NotImplementedError



if __name__ == '__main__':
    unittest.main()
