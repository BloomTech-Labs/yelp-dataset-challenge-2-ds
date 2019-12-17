"""
Read Queries

Database read functionality for Scraper.
"""
from db import get_session
from models import *

import logging

read_logger = logging.getLogger(__name__)


####################
### Read Queries ###
####################

def sample_data(coordinates, model_radius=0.05):
    with get_session() as session:
        response = session.query(SearchResults.latitude, SearchResults.longitude, \
            SearchResults.category, SearchResults.num_unique).\
                filter((SearchResults.latitude-coordinates[0] < model_radius),
                        (SearchResults.longitude-coordinates[1] < model_radius)).all()
        return response