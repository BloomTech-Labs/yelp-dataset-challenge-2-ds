"""
Yelp Scraper Part 1
    This module utilizes the Yelp Fusion API to find businesses
    within a geographic area centered on a city.
"""

from yelpapi import YelpAPI
from decouple import config
import os
import numpy as np
import pandas as pd

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
        g.environment = True
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


def check_environment():
    if hasattr(g, 'environment'):
        return True
    return False


#####################
### Data Handling ###
#####################

def clean_business_search(df: pd.DataFrame):
    temp = df.copy()
    # Filter and Rename Columns
    temp = df.filter(['id', 'name', 'image_url', 'coordinates', \
                      'review_count', 'is_closed', 'url', 'categories',\
                      'location', 'rating'])
    temp = temp.rename(columns={'id':'business_id', 'rating': 'stars'})
    
    # change is_closed to is_open (flip bool)
    temp['is_open'] = temp.is_closed.apply(lambda x: not x)
    temp = temp.drop(columns='is_closed')
    
    # parse location to address, city, state, postal_code
    temp['address'] = temp.location.apply(lambda x: str(x['address1'])+str(x['address2']))
    temp['city'] = temp.location.apply(lambda x: x['city'])
    temp['state'] = temp.location.apply(lambda x: x['state'])
    temp['postal_code'] = temp.location.apply(lambda x: x['zip_code'])
    temp = temp.drop(columns='location')
    
    # clean categories down to alias (more similar to parent search)
    temp.categories = temp.categories.apply(lambda x: ','.join([z['alias'] for z in x]))
    
    # parse coordinates
    temp['latitude'] = temp.coordinates.apply(lambda x: x['latitude'])
    temp['longitude'] = temp.coordinates.apply(lambda x: x['longitude'])
    temp = temp.drop(columns='coordinates')
    
    return temp


def search(category, latitude, longitude):
    # Check environment
    if not check_environment():
        load_environment(from_file=True)
    # Get client and run search
    client = get_client()
    search_results = client.search_query(
        categories=category, latitude=latitude, longitude=longitude, limit=50
        )
    df = pd.DataFrame(search_results['businesses'])
    return clean_business_search(df)


if __name__ == "__main__":
    load_environment(from_file=True)

    client = get_client()

    response = client.search_query(term='ice cream', location='austin, tx', sort_by='rating', limit=5)
    print(response)