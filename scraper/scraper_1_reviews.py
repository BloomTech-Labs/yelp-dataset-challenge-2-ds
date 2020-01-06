"""
Scraper 1: Reviews
    Use ListScraper to query Yelp Fusion API for available reviews and write to database.
"""

from read_query import non_reviewed_businesses
from yelp import get_client
from write_query import write_review
from datetime import datetime
import numpy as np
import time

import logging

review_logger = logging.getLogger(__name__)

#######################
### Search Function ###
#######################

def review_search(id: str):
    time.sleep(np.random.randint(2,5))  # Yelp FusionAPI seems ok with rand(2,5) delay
    client = get_client()
    response = client.reviews_query(id)
    return clean_review_response(response=response, business_id=id)


#####################
### Data Handling ###
#####################

def clean_review_response(response: dict, business_id: str):
    reviews = []
    for review in response['reviews']:
        reviews.append({
            'review_id': review['id'],
            'date': convert_to_datetime(review['time_created']),
            'text': review['text'],
            'user_id': review['user']['id'],
            'business_id': business_id
        })
    return reviews


def save_reviews(reviews: list):
    for review in reviews:
        write_review(review)


def convert_to_datetime(time_string):
    review_logger.debug('Converting {} to datetime'.format(time_string))
    try:
        assert type(time_string) == str
        return datetime.fromisoformat(time_string)
    except ValueError:
        review_logger.debug('Value Error: Invalid isoformat string')
        review_logger.debug('Sending default datetime')
        return datetime.fromisoformat('1969-01-01')
    except AssertionError:
        review_logger.debug('AssertionError: DateTime not a string.')
        review_logger.debug('Attempting translation')
        return datetime.fromtimestamp(time_string / 1e3)
    except:
        raise