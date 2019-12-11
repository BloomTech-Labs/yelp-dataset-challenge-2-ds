"""
Write Queries

Database write functionality for Scraper
"""

from db import get_session
from models import *

import logging


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
        