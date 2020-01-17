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

def list_categories(with_id=False):
    with get_session() as session:
        if with_id:
            response = session.query(Category.cat_id, Category.cat_name).all()
            return {x[1]: x[0] for x in response}
        else:
            response = session.query(Category.cat_name).all()
            return [x[0] for x in response]


def get_near_data(center_coord, radius):
    # Square search.  Returns data within a box.
    lat_range = [center_coord[0]-radius, center_coord[0]+radius]
    lon_range = [center_coord[1]-radius, center_coord[1]+radius]
    
    with get_session() as session:
        response = session.query(
            SearchResults.latitude, SearchResults.longitude,
            SearchResults.category, SearchResults.num_unique).\
                filter(
                    SearchResults.latitude > lat_range[0],
                    SearchResults.latitude < lat_range[1],
                    SearchResults.longitude > lon_range[0],
                    SearchResults.longitude < lon_range[1]).all()
    return response


def dump_businesses():
    with get_session() as session:
        response = session.query(
            Business.business_id, Business.name, Business.address,
            Business.city, Business.state, Business.latitude, Business.longitude,
            Business.postal_code, Business.review_count, Business.stars, Business.is_open,
            Business.categories
        ).all()
    return response


def dump_reviews():
    with get_session() as session:
        response = session.query(
            Review.review_id, Review.date, Review.stars, Review.text,
            Review.business_id, Review.user_id
        ).all()
    return response


def non_reviewed_businesses():
    with get_session() as session:
        review_business_ids = session.query(
            Review.business_id
        ).distinct()
        response = session.query(
            Business.business_id
        ).filter(Business.business_id.notin_(review_business_ids)).all()
    return [x[0] for x in response]
