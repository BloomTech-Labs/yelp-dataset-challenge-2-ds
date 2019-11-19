"""
S3 is an abstraction layer for working with s3 buckets. Uses helper functions to manage connection
    and exposes a read/write api.  Built on Boto3.s3 commands.
"""
import boto3
from boto3.s3.transfer import S3Transfer
from botocore.exceptions import ClientError
from io import BytesIO, TextIOWrapper
import logging
import os
import sys
import threading
from contextlib import contextmanager
from decouple import config

class Bucket():

    def __init__(self, bucket_name, config_type='file'):
        """Wrapper around boto3.s3 commands.  Assists in env setup across
            platforms by guiding user through credential setup and client gen.

            :type config_type: str
            :param config_type: Type of configuration style.
                'file': generates credential file
                'env': looks for credentials in os environment
        """
        self.bucket_name = bucket_name
        self.setup_env(config_type)
        self.contents = self.dir(all=True)

    def __repr__(self):
        return 'AWS S3 Bucket <{}>'.format(self.bucket_name)

    def setup_env(self, config_type):
        try:
            credential_file = os.path.join(os.getcwd(), '.aws', 'credentials')
            load_aws_environment_file(credential_file=credential_file)
        except:
            print('Could not find credential file. Defaulting to manual setup.')
            set_aws_environ()

    def get(self, object_name, save_name=None, **kwargs):
        return download_file(
            bucket_name=self.bucket_name,
            object_name=object_name,
            save_name=save_name,
            kwargs=kwargs,
            )

    def save(self, file_name, object_name=None):
        upload_file(file_name, self.bucket_name, object_name=object_name)

    def delete(self, object_name):
        delete_object(bucket_name=self.bucket_name, object_name=object_name)

    def dir(self, all=False):
        if all:
            keys = []
            for key in get_bucket_keys(self.bucket_name):
                keys.append(key)
            return keys
        return get_bucket_keys(self.bucket_name)

    def get_dir_contents(self, dir):
        with get_client() as conn:
            objects = conn.list_objects_v2(Bucket=self.bucket_name, Prefix=dir)['Contents']
            contents = []
            for key in objects:
                contents.append(key)

        # If the search returned something, slice off the first result because that will be the directory itself
        if contents != []:
            return contents[1:]

        else:
            raise NameError("No directory starts with " + dir + " prefix.")

    def find(self, search=None, prefix=None, suffix=None):
        return get_matching_s3_keys(
            bucket_contents=self.contents,
            search=search,
            prefix=prefix,
            suffix=suffix)


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


@contextmanager
def get_client(setup=False):
    if setup:
        setup_aws()
    # Setup connection with credentials and yield created client.
    #   Allows for usage: with get_client() as client: client...
    try:
        client = boto3.client('s3')
    except:
        print('Client could not read credential file.  Looking for os environment variables')
        client = boto3.client(
            's3',
            aws_access_key_id=os.environ["aws_access_key_id"],
            aws_secret_access_key=os.environ["aws_secret_access_key"]
            )
        print('Could not establish client')
        raise
    yield client


def setup_aws(key_id=None, secret_key=None, region=None):
    """
    Interactive config file creation.
        Stores credentials in ~/.aws/credentials
    """
    key_id = 'aws_access_key_id = ' + key_id
    secret_key = 'aws_secret_access_key = ' + secret_key

    if key_id is None:
        key_id = 'aws_access_key_id = ' + input("Enter your aws_access_key_id: ")
    if secret_key is None:
        secret_key = 'aws_secret_access_key = ' + input("Enter your aws_secret_access_key: ")

    # Set config
    profile = '[default]'
    if not region is None:
        region = check_region(
            input('Enter region: ').lower()
            )
        region = 'region = ' + region
    else:
        region = ''

    # Create env files
    create_directory()
    root_path = os.path.join(os.getcwd(), '.aws')
    cred_path = root_path + '/credentials'
    config_path = root_path + '/config'
    create_file(cred_path, "\n".join([profile, key_id, secret_key]))
    create_file(config_path, "\n".join([profile, region]))
    # Load env files
    load_aws_environment_file(credential_file=cred_path, profile='default')


def load_aws_environment_file(credential_file=None, profile='default'):
    """Set local environment variables.  Can be run alone if
    credential_file already created.

    :type credential_file: file
    :param credential_file: A non-suffixed file containing access key,
        secret access key, and profiles.

    :type profile: str
    :param: The profile to load from credential file.
    """
    if credential_file is None:
        try:
            aws_access_key_id=config('AWS_ACCESS_KEY'),
            aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY')
        except OSError as error:
            raise error
        except:
            print('An unknown error has occured')
            raise
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = credential_file
    os.environ['AWS_PROFILE'] = profile

def set_aws_environ(key_id=None, secret_key=None):
    if key_id is None or secret_key is None:
        key_id = 'aws_access_key_id = ' + input("Enter your aws_access_key_id: ")
        secret_key = 'aws_secret_access_key = ' + input("Enter your aws_secret_access_key: ")
    os.environ["aws_access_key_id"] = key_id
    os.environ["aws_secret_access_key"] = secret_key

def create_file(filename, line):
    """
    part of setup_aws. Creates files at filename.  Only takes single line file streams.
        Meant to create environment files for aws credentials, config.
    """
    try:
        with open(filename, 'w+') as f:
            f.write(line)
    except:
        print('Could not write {}'.format(filename))


def create_directory(directory_name='.aws'):
    """
    Part of setup_aws.  Creates a directory.  Can create arbitrary directories as well by
        replacing directory_name
    """
    path = os.path.join(os.getcwd(), directory_name)
    try:
        if not os.path.exists(path):
            os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed." % path)
    else:
        print ("Successfully created the directory %s." % path)


def get_regions(region_file='aws_regions.txt'):
    """
    Returns a list of availble regions.
    """
    return [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ap-east-1",
        "ap-south-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "ap-northeast-2",
        "ca-central-1",
        "cn-north-1",
        "cn-northwest-1",
        "eu-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-north-1",
        "me-south-1",
    ]

def check_region(region):
    """
    Check available regions for input validation.  Returns self if valid, else recursively asks for new region.
    """
    if region in get_regions():
        return region
    else:
        print('Invalid Region.  Region can be: {}'.format(get_regions))
        region = input('Please type a region: ')
        check_region(region)


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            region = check_region(region)
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def list_buckets(setup=False):
    """
    List buckets available to client.

    :type setup: bool
    :params setup: Calls load_aws_env to setup client if True
    """
    if setup:
        credential_file = os.path.join(os.getcwd(), '.aws') + '/credentials'
        load_aws_environment_file(credential_file=credential_file)

    with get_client() as connection:
        response = connection.list_buckets()

    for bucket in response['Buckets']:
        print(f'    {bucket["Name"]}')


def upload_file(file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_path

    # Upload the file
    with get_client() as client:
        try:
            transfer = S3Transfer(client)
            transfer.upload_file(file_path, bucket, object_name,
                                    callback=ProgressPercentage(file_path))
        except ClientError as e:
            logging.error(e)
            return False
        return True

def download_file(bucket_name, object_name, save_name=None, **kwargs):
    # Download file to memory if save_name == None
    #   This may not work for non string-representative data.
    #   Invokes S3Transfer method for
    with get_client() as connection:
        if save_name is None:
            # Create filestream to store temporary object
            file_stream = BytesIO()
            connection.download_fileobj(
                Bucket=bucket_name,
                Key=object_name,
                Fileobj=file_stream
                )
            return file_stream

        else:
            transfer = S3Transfer(connection)
            transfer.download_file(
                bucket_name, object_name, save_name
                )


def delete_object(bucket_name, object_name):
#     """Delete an object from an S3 bucket

#     :param bucket_name: string
#     :param object_name: string
#     :return: True if the referenced object was deleted, otherwise False
#     """
    # Delete the object
    with get_client() as connection:
        try:
            connection.delete_object(Key=object_name, Bucket=bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


def get_bucket_keys(bucket_name, prefix='', suffix='', max=100, all=False):
    """
    Return generator that yields keys in S3 bucket.
    **Adapted from https://alexwlchan.net/2017/07/listing-s3-keys/
    """
    with get_client() as connection:
        response = connection.list_objects_v2(Bucket=bucket_name)

    for obj in response['Contents']:
        key = obj['Key']
        if key.startswith(prefix) and key.endswith(suffix):
            yield key
    return response

## Adaped from https://alexwlchan.net/2019/07/listing-s3-keys/
## Special thanks to Alex Chan
def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    with get_client() as client:
        paginator = client.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj

def get_matching_s3_keys(bucket_contents, search=None, prefix=None, suffix=None):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    find_list = []
    for item in bucket_contents:
        test_item = item
        search_status = False
        if search: # If general search, screen for term right away
            if search in test_item:
                find_list.append(test_item)
                search_status = True # remember search hit to pop if necessary
            else:
                test_item = None

        if prefix and test_item:
            if test_item.startswith(prefix):
                if not search_status:
                    find_list.append(test_item)
            else:
                if search_status:
                    find_list.pop()
                test_item = None

        if suffix and test_item:
            if test_item.endswith(suffix):
                if not search_status:
                    find_list.append(test_item)
            else:
                if search_status:
                    find_list.pop()
    return find_list

if __name__ == "__main__":
    pass
    # credential_file = os.path.join(os.getcwd(), '.aws') + '/credentials'
    # load_aws_environment(credential_file=credential_file)

    # conn = get_client()
    # response = conn.list_buckets()

    # for bucket in response['Buckets']:
    #     print(f'    {bucket["Name"]}')