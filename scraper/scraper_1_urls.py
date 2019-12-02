"""
Yelp Scraper Part 1
    This module utilizes the Yelp Fusion API to find businesses
    within a geographic area centered on a city.
"""

from yelpapi import YelpAPI
from decouple import config
import os
from db import get_db, get_session

from app_global import g


#########################
### Environment Setup ###
#########################

def read_credentials(filepath='.yelp/credentials'):
    """Read credential file and return dictionary of important information"""
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
        print('Environment variables set.')
    else:
        print('Environment Loading Without Credential File Not Currently Supported.  See read_credentials.')
        raise NotImplementedError


##############################
### Yelp Control Functions ###
##############################

def get_client(**kwargs):
    if g.client == None:
        client = YelpAPI(config('api_key'))
        g.client = client
        return g.client
    return g.client


#####################
### Data Handling ###
#####################

def impute_missing(y: np.ndarray, coordinates: tuple):
    """Query nearby data and build localized dataframe to impute y"""
    pass


if __name__ == "__main__":
    load_environment(from_file=True)

    client = get_client()

    response = client.search_query(term='ice cream', location='austin, tx', sort_by='rating', limit=5)
    print(response)