"""
Test file with example usage for awstools.  Built for yelp data challenge.

"""
import os
from awstools import s3

# Setup AWS credentials and create local credential files.
# s3.setup_aws()

# Initialize bucket
bucket = s3.Bucket('yelp-data-shared-labs18')

# Test download of file.  test.txt contains lorem ipsum text.
bucket.get('test.txt', 'text.txt')

# Test stream file (download to variable)
text = bucket.get('test.txt')
print(text)

# Test upload (saving)
test_path = os.path.join(os.getcwd(), 'text.txt')
bucket.save(test_path, 'upload_test.txt')