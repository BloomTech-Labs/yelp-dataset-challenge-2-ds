import logging
from datetime import datetime
# from multiprocessing import Pool
from models import *
from db import get_session, get_db
from flask import current_app, g
import numpy as np
import time
import ujson
import re


query_logger = logging.getLogger(__name__)

class Query():
    def __init__(self, query):
        self.query = query

    def check_constraints(self, record, session):
        """Check for constraints in a record.
            If any found, create empty rows with needed keys to enforce relational constraints.
            Also handle known typing difficulties with SQL Alchemy (datetime)
        """

        # DateTime Checks
        query_logger.debug('Checking for datetime fields')
        for field in ['date', 'yelping_since']:
            if field in record.keys():
                record[field] = convert_to_datetime(record[field])

        # Foreign Key Checks
        if 'business_id' in record.keys():
            query_logger.debug('business_id found in query.  Checking for existing record.')
            exists = session.query(Business).filter_by(business_id=record['business_id']).scalar() is not None
            if not exists:
                query_logger.info('business_id did not return existing row. Generating empty row.')
                business = Business(business_id=record['business_id'])
                session.add(business)
                session.commit()

        if 'user_id' in record.keys():
            query_logger.debug('user_id found in query.  Checking for existing record.')
            exists = session.query(User).filter_by(user_id=record['user_id']).scalar() is not None
            if not exists:
                query_logger.info('user_id did not return existing row. Generating empty row.')
                user = User(user_id=record['user_id'])
                session.add(user)
                session.commit()


    def fill(self, data):
        NotImplemented

    def execute(self):
        NotImplemented


class Get(Query):
    def __init__(self, query):
        super().__init__(query)
        query_logger.info('GET query created.')
        self.maker = assign_maker(query['schema'])
        query_logger.info('Maker for schema {} initialized'.format(query['schema']))
        self.execute(query['params'])

    def execute(self, params):
        assert type(params) == dict
        query_logger.info('GET query launching params = {}'.format(params))
        with get_session() as session:
            self.response = self.maker(session=session, params=params)


class Post(Query):
    def __init__(self, query):
        super().__init__(query)
        query_logger.info('POST query created')
        query_logger.debug('Query: {}'.format(query))
        self.maker = assign_maker(query['table_name'])
        self.execute(query['data'])

    def execute(self, records):
        assert type(records) == list
        with get_session() as session:
            query_logger.info('Adding {} records to session stack.'.format(len(records)))
            for record in records:
                self.check_constraints(record=record, session=session)
                self.maker(record=record, session=session)
            query_logger.info('Stack comitted')
            session.commit()


###################
###Query Methods###
###################

# Process function to map onto databunch
def query_database(method, query):
    """Query handler for sqlalchemy database.  Parse tablename and direct query.
    """
    query_logger = logging.getLogger(__name__ + '.query_database')
    query_logger.info("Query Received.  Method: {}  DataType: {}".format(method, type(query)))
    query_logger.debug(query)

    if method == 'GET':
        query = Get(query=query)
        return query.response
    elif method == 'POST':
        run_post(query=query)  # Single-threaded operation

    return {'message': 'POST received and executed'}


def run_post(query):
    time.sleep(np.random.random_sample())
    return Post(query=query)


def convert_to_datetime(time_string):
    query_logger.debug('Converting {} to datetime'.format(time_string))
    try:
        assert type(time_string) == str
        return datetime.fromisoformat(time_string)
    except ValueError:
        query_logger.debug('Value Error: Invalid isoformat string')
        query_logger.debug('Sending default datetime')
        return datetime.fromisoformat('1969-01-01')
    except AssertionError:
        query_logger.debug('AssertionError: DateTime not a string.')
        query_logger.debug('Attempting translation')
        return datetime.fromtimestamp(time_string / 1e3)
    except:
        raise


def build_databunch(query, num_splits=3):
    databunch = []
    bunch_size = int(len(query['data']) / num_splits)
    for i in range(num_splits):
        if i < num_splits-1:
            data_range = (i*bunch_size, (i+1)*bunch_size)
        else:
            data_range = (i*bunch_size, len(query['data']))
        databunch.append(
            {
                'table_name': query['table_name'],
                'data': query['data'][data_range[0]:data_range[1]]
            }
        )
    return databunch

###########################
###Make Instance Methods###
###########################

# TODO: Collapse into single maker factory that calls proper class
def assign_maker(schema):
    makers = {
        'businesses': make_or_update_business,
        'users': make_or_update_user,
        'checkins': make_or_update_checkin,
        'photos': make_or_update_photo,
        'tips': make_or_update_tip,
        'reviews': make_or_update_review,
        'review_sentiment': make_or_update_review_sentiment,
        'tip_sentiment': make_or_update_tip_sentiment,
        'viz2': make_or_update_viz2,
        'biz_words': biz_words,
        'biz_comp': biz_comp,
    }
    return makers[schema]


def make_or_update_business(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Business).filter_by(business_id=record['business_id']).scalar() is not None
    if not exists:
        query_logger.debug('business_id did not return existing row. Creating new business instance')
        session.add(Business(**record))
    else:
        session.query(Business).filter_by(business_id=record['business_id']).update(record)


def make_or_update_user(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(User).filter_by(user_id=record['user_id']).scalar() is not None
    if not exists:
        query_logger.debug('user_id did not return existing row. Creating new business instance')
        session.add(User(**record))
    else:
        session.query(User).filter_by(user_id=record['user_id']).update(record)


def make_or_update_checkin(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Checkin).filter_by(checkin_id=record['checkin_id']).scalar() is not None
    if not exists:
        query_logger.debug('checkin_id did not return existing row. Creating new business instance')
        session.add(Checkin(**record))
    else:
        session.query(Checkin).filter_by(checkin_id=record['checkin_id']).update(record)


def make_or_update_photo(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Photo).filter_by(photo_id=record['photo_id']).scalar() is not None
    if not exists:
        query_logger.debug('photo_id did not return existing row. Creating new business instance')
        session.add(Photo(**record))
    else:
        session.query(Photo).filter_by(photo_id=record['photo_id']).update(record)


def make_or_update_tip(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Tip).filter_by(tip_id=record['tip_id']).scalar() is not None
    if not exists:
        query_logger.debug('tip_id did not return existing row. Creating new business instance')
        session.add(Tip(**record))
    else:
        session.query(Tip).filter_by(tip_id=record['tip_id']).update(record)


def make_or_update_review(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Review).filter_by(review_id=record['review_id']).scalar() is not None
    if not exists:
        query_logger.debug('review_id did not return existing row. Creating new business instance')
        session.add(Review(**record))
    else:
        session.query(Review).filter_by(review_id=record['review_id']).update(record)


def make_or_update_review_sentiment(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    try:
        exists = session.query(ReviewSentiment).filter_by(review_id=record['review_id']).scalar() is not None
    except:
        query_logger.info('Error in .scalar(). Multiple Found?.  Exception in alchemy ret one()')
        query_logger.info('')
        exists = False
    if not exists:
        query_logger.debug('review_id did not return existing row. Creating new business instance')
        session.add(ReviewSentiment(**record))
    else:
        session.query(ReviewSentiment).filter_by(review_id=record['review_id']).update(record)


def make_or_update_tip_sentiment(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    try:
        exists = session.query(TipSentiment).filter_by(tip_id=record['tip_id']).scalar() is not None
    except:
        query_logger.info('Error in .scalar(). Multiple Found?. Exception in alchemy ret one()')
        exists = False
    if not exists:
        query_logger.debug('tip_id did not return existing row. Creating new business instance')
        session.add(TipSentiment(**record))
    else:
        session.query(TipSentiment).filter_by(tip_id=record['tip_id']).update(record)


def make_or_update_viz2(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    for key in record.keys():
        if type(record[key]) == dict:
            record[key] = str(record[key])
    try:
        exists = session.query(Viz2).filter_by(business_id=record['business_id']).scalar() is not None
    except:
        query_logger.info('Error in .scalar(). Multiple Found?. Exception in alchemy ret one()')
        exists = False
    if not exists:
        query_logger.debug('business_id did not return existing row. Creating new viz2 instance')
        session.add(Viz2(**record))
    else:
        session.query(Viz2).filter_by(business_id=record['business_id']).update(record)


# GET ENDPOINT FUNCTIONS #
# ------------------------

def biz_words(session, params, *args, **kwargs):
    response = session.query(Review.date, Review.token, Review.stars).\
        filter(Review.business_id==params['business_id']).order_by(Review.date)
    return {'data': response.all()}


def biz_comp(session, params, *args, **kwargs):
    # Join select business information to viz2 aggregation data on business_id
    response = session.query(
            Business.business_id, Business.address, Business.city, Business.state,
            Business.postal_code, Business.review_count, Viz2.categories, Viz2.percentile,
            Viz2.competitors, Viz2.bestinsector, Viz2.avg_stars_over_time, Viz2.chunk_sentiment,
            Viz2.count_by_star, Viz2.review_by_year).\
            join(Viz2).filter(Business.business_id == params['business_id']).all()
    
    # Get first row (could use first() as well) and package output for easier processing
    response = response[0]
    # Parse nested fields into more readible format
    def get_components(element):
        return re.findall(r'\[([^]]+)\]', element)
    def strip_json_artifacts(element):
        return element.strip().strip("'")

    avg_stars_components = get_components(response.avg_stars_over_time)
    dates = [strip_json_artifacts(x) for x in avg_stars_components[1].split(',')]
    avg_stars = [float(x) for x in avg_stars_components[0].split(',')]
    
    chunk_sentiment_components = get_components(response.chunk_sentiment)
    noun_chunks = [strip_json_artifacts(x) for x in chunk_sentiment_components[0].split(',')]
    chunk_sentiment = [float(strip_json_artifacts(x)) for x in chunk_sentiment_components[1].split(',')]

    package = {
        'business_id': response.business_id,
        'address': response.address,
        'city': response.city,
        'state': response.state,
        'postal_code': response.postal_code,
        'review_count': response.review_count,
        'categories': response.categories,
        'percentile': response.percentile,
        'competitors': [strip_json_artifacts(x) for x in ujson.loads(
                        response.competitors).strip('[]').split(',')],
        'bestinsector': [strip_json_artifacts(x) for x in ujson.loads(
                        response.bestinsector).strip("[]").split(',')],
        'avg_stars': avg_stars,
        'dates': dates,
        'noun_chunks': noun_chunks,
        'chunk_sentiment': chunk_sentiment,
        'count_by_star': eval(response.count_by_star), 
        'review_by_year': eval(response.review_by_year),
    }

    return package