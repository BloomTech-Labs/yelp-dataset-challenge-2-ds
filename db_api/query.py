import logging
from datetime import datetime
# from multiprocessing import Pool
from models import *
from db import get_session, get_db
from flask import current_app, g
import numpy as np
import time


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

    # num_splits = 2  # multi-threaded session operation
    # Check query data size.  If small enough, this number of splits may cause pool issues.
    # if len(query['data']) < num_splits:
    #     num_splits = len(query['data'])

    if method == 'GET':
        query = Get(query=query)
        return query.response
    elif method == 'POST':
        run_post(query=query)  # Single-threaded operation
        # databunch = build_databunch(query=query, num_splits=num_splits) # Split data
        # p = Pool(len(databunch))
        # p.map(run_post, databunch)

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

# TODO: Collapse into single makre factory that calls proper class
def assign_maker(schema):
    makers = {
        'businesses': make_or_update_business,
        'users': make_or_update_user,
        'checkins': make_or_update_checkin,
        'photos': make_or_update_photo,
        'tips': make_or_update_tip,
        'reviews': make_or_update_review,
        'biz_words': biz_words,
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


# GET ENDPOINT FUNCTIONS #
# ------------------------

def biz_words(session, params, *args, **kwargs):
    response = session.query(Review.date, Review.token, Review.stars).\
        filter(Review.business_id==params['business_id']).order_by(Review.date)
    return {'data': response.all()}