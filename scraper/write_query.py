"""
Write Queries

Database write functionality for Scraper
"""

from db import get_session
from models import *

import logging

write_logger = logging.getLogger(__name__)

########################
### Helper Functions ###
########################

def filter_unique(raw_frame):
    ## Check if id existing, if exists: drop, else keep
    raw_frame['exists'] = raw_frame.business_id.apply(check_exists)
    
    return raw_frame.query('exists == False').drop(columns='exists')

def check_exists(x):
    # Check if business_id already in database
    with get_session() as session:
        exists = session.query(Business.business_id).filter(
            Business.business_id == x
        ).scalar()
        
    if exists:
        return True
    else:
        return False


#####################
### Write Queries ###
#####################

def write_business_search(unique_frame):
    # Only pass records with unique id's to this function.
    with get_session() as session:
        for record in unique_frame.to_dict(orient='r'):
            session.add(Business(**record))
            session.commit()


def write_search_metadata(**kwargs):
    with get_session() as session:
        session.add(SearchResults(**kwargs['record']))
        session.commit()


def write_categories(category_list):
    with get_session() as session:
        for category in category_list:
            session.add(Category(cat_name=category))
            session.commit()


def write_perceptron_metadata(record):
    # Check if existing to UPDATE or INSERT
    with get_session() as session:
        try:
            exists = session.query(Perceptron).filter_by(geohash=record['geohash']).scalar() is not None
        except:
            write_logger.info('Error in .scalar(). Multiple Found?. Exception in alchemy ret one()')
            exists = False
        if not exists:
            write_logger.debug('geohash did not return existing row. Creating new business instance')
            session.add(Perceptron(**record))
        else:
            session.query(Perceptron).filter_by(geohash=record['geohash']).update(record)
        session.commit()