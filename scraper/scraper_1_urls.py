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
import time
import logging
from read_query import list_categories
from write_query import (filter_unique, write_business_search, write_search_metadata,
                            write_categories)
from yelp import get_client
from scrapers import GeoScraper

from app_global import g


scraper_url_logger = logging.getLogger(__name__)


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

###########################
### Bootstrap Functions ###
###########################

def load_categories(filename = 'categories.json'):
    categories = pd.read_json(filename)
    category_list = categories.query("country == 'US'").parent.unique().tolist()
    write_categories(category_list=category_list)
    

def bootstrap_search(center_coord: tuple):
    # Get active categories
    categories = list_categories()
    # Generate area map (model positions)
    g.modelmap = lens.ModelMap(
        center_coord = center_coord,
        map_radius=0.5,
        model_radius=0.1,
    )
    # Initialize scrapers
    g.scrapers = []
    for category in categories:
        g.scrapers.append(
            GeoScraper(
                start_coord = g.modelmap.map[0],
                radius=1,
                category=category
                )
            )   
    
    # Run on random set of model coordinates to fill map in
    for index in np.random.choice(len(g.modelmap.map), int(len(g.modelmap.map)/20), replace=False):
        print('Search at {}'.format(g.modelmap.map[index]))
        scraper_url_logger.info('Searching all categories at {}'.format(g.modelmap.map[index]))
        for scraper in g.scrapers:
            scraper.coordinates = g.modelmap.map[index]
            try:
                scraper.search()
            except:
                scraper_url_logger.error('YelpAPIError: INTERNAL_ERROR.  Skipping search.')
            time.sleep(np.random.randint(2,5))  # Yelp FusionAPI seems ok with rand(2,5) delay




if __name__ == "__main__":
    client = get_client()

    response = client.search_query(term='ice cream', location='austin, tx', sort_by='rating', limit=5)
    print(response)