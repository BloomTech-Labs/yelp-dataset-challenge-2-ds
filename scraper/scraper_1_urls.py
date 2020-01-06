"""
Yelp Scraper Part 1: URLs
    This module contains scraper and data cleaning logic to control
    business search and data cleaning/transformation.
"""

import numpy as np
import pandas as pd
from yelp import get_client
import logging


scraper_url_logger = logging.getLogger(__name__)


#######################
### Search Function ###
#######################

def geo_search(category, latitude, longitude):
    # Get client and run search
    client = get_client()
    search_results = client.search_query(
        categories=category, latitude=latitude, longitude=longitude, limit=50
        )
    df = pd.DataFrame(search_results['businesses'])
    return clean_business_search(df)


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
    try:
        temp['is_open'] = temp.is_closed.apply(lambda x: not x)
        temp = temp.drop(columns='is_closed')
    except:
        scraper_url_logger.error('is_closed not found.  Setting to invalid = 2')
        temp['is_open'] = 2
    
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