"""
Test file with example usage for awstools.  Built for yelp data challenge.

Includes tests for s3 and jobs

"""
import os
import s3
import tracemalloc
import unittest
import warnings

###############
### s3 Tests###
###############

## Current version may require .aws folder and credential file be created.
    ## Setup AWS credentials and create local credential files.
# s3.setup_aws()

class TestS3(unittest.TestCase):

    def setUp(self):
        # Initialize bucket
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        self.bucket = s3.Bucket('yelp-data-shared-labs18')

    def test_bucket_creation(self):
        # Check contents returns non-null (bucket selected must not be empty)
        self.assertIsNotNone(self.bucket.contents)

    # def test_bucket_upload(self):
    #     # test_path = os.path.join(os.getcwd(), 'text.txt')
    #     # bucket.save(test_path, 'upload_test.txt')
    #     raise NotImplementedError

    # def test_bucket_download(self):
    #     # Test download of file.  test.txt contains lorem ipsum text.
    #     raise NotImplementedError

    # def test_bucket_stream(self):
        # Test stream file (download to variable)
        # text = bucket.get('test.txt')
        # print(text)
        # raise NotImplementedError



if __name__ == '__main__':
    tracemalloc.start()
    unittest.main()

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)