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
    # Get existing id's
    raise NotImplementedError # INCOMLETE
    with get_session() as session:
        id_list = session.query(Business.business_id).all()


#####################
### Write Queries ###
#####################

def write_business_search(unique_frame):
    # Only pass records with unique id's to this function.
    with get_session() as session:
        for record in unique_frame.to_dict(orient='r'):
            session.add(Business(**record))
        
        session.commit()
        session.close()