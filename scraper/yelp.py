from app_global import g
from yelpapi import YelpAPI
from decouple import config
import os

import logging


yelp_logger = logging.getLogger(__name__)

#########################
### Environment Setup ###
#########################

def read_credentials(filepath='.yelp/credentials'):
    """Read credential file and return dictionary of important information"""
    yelp_logger.info("Reading in Credentials")
    with open(filepath, 'r') as f:
        contents = f.readlines()

    credentials = {}
    key_items = ['client_id', 'api_key']
    for line in contents:
        for item in key_items:
            if item in line:
                credentials[item] = line.split('=')[1].strip()

    return credentials


def load_environment(from_file=False):
    """Load credentials into environment if necessary"""
    if from_file==True:
        credentials = read_credentials()
        for key in credentials.keys():
            os.environ[key] = credentials[key]
        g.environment = True
        yelp_logger.info('Environment variables set.')
        print('Environment variables set.')
    else:
        yelp_logger.error('Environment Loading Without Credential File Not Currently Supported.  See read_credentials.')
        print('Environment Loading Without Credential File Not Currently Supported.  See read_credentials.')
        raise NotImplementedError


##############################
### Yelp Control Functions ###
##############################

def get_client(**kwargs):
    yelp_logger.debug('Yelp client request received')
    if g.client == None:
        # Check environment
        if not check_environment():
            load_environment(from_file=True)
        client = YelpAPI(config('api_key'))
        g.client = client
        return g.client
    return g.client


def check_environment():
    if hasattr(g, 'environment'):
        return True
    return False