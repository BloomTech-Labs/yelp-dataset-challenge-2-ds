import logging
from models import *

query_logger = logging.getLogger(__name__)

class Query():
    def __init__(self, session, query):
        self.session = session
        self.query = query

    def check_constraints(self, record):
        """Check for constraints in a record.
            If any found, create empty rows with needed keys to enforce relational constraints.
        """
        if 'businessid' in record.keys():
            query_logger.info('BusinessID found in query.  Checking for existing record.')
            exists = self.session.query(Business).filter_by(businessid=record['businessid']).scalar() is not None
            if not exists:
                query_logger.info('businessid did not return existing row. Generating empty row.')
                business = Business(businessid=record['businessid'])
                self.session.add(business)

        if 'userid' in record.keys():
            query_logger('UserID found in query.  Checking for existing record.')
            exists = self.session.query(User).filter_by(userid=record['userid']).scalar() is not None
            if not exists:
                query_logger.info('userid did not return existing row. Generating empty row.')
                user = User(userid=record['userid'])
                self.session.add(user)

    def fill(self, data):
        NotImplemented

    def execute(self):
        NotImplemented


class Get(Query):
    def __init__(self, session, query):
        super().__init__(session, query)
        query_logger.info('GET query created.')
        self.contents_ = None  # Generate from execution step


class Post(Query):
    def __init__(self, session, query):
        super().__init__(session, query)
        query_logger.info('POST query created')
        query_logger.info('Session: {} Query: {}'.format(session, query))
        self.query = query
        self.maker = assign_maker(query['table_name'])
        self.fill(query['data'])


    def fill(self, records):
        assert type(records) == list
        query_logger.info('Adding {} records to session stack.'.format(len(records)))
        for record in records:
            self.check_constraints(record)
            self.maker(self.session, record)

    def execute(self):
        self.session.commit()


###########################
###Make Instance Methods###
###########################
# TODO: Collapse into single makre factory that calls proper class
def assign_maker(table_name):
    makers = {
        'businesses': make_or_update_business,
        'users': make_or_update_user,
        'checkins': make_or_update_checkin,
        'photos': make_or_update_photo,
        'tips': make_or_update_tip,
        'reviews': make_or_update_review,
    }
    return makers[table_name]


def make_or_update_business(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Business).filter_by(businessid=record['businessid']).scalar() is not None
    if not exists:
        query_logger.info('businessid did not return existing row. Creating new business instance')
        session.add(Business(**record))
    else:
        session.query(Business).filter_by(businessid=record['businessid']).update(record)


def make_or_update_user(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(User).filter_by(userid=record['userid']).scalar() is not None
    if not exists:
        query_logger.info('userid did not return existing row. Creating new business instance')
        session.add(User(**record))
    else:
        session.query(User).filter_by(userid=record['userid']).update(record)


def make_or_update_checkin(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Checkin).filter_by(checkinid=record['checkinid']).scalar() is not None
    if not exists:
        query_logger.info('checkinid did not return existing row. Creating new business instance')
        session.add(Checkin(**record))
    else:
        session.query(Checkin).filter_by(checkinid=record['checkinid']).update(record)


def make_or_update_photo(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Photo).filter_by(photoid=record['photoid']).scalar() is not None
    if not exists:
        query_logger.info('photoid did not return existing row. Creating new business instance')
        session.add(Photo(**record))
    else:
        session.query(Photo).filter_by(photoid=record['photoid']).update(record)


def make_or_update_tip(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Tip).filter_by(tipid=record['tipid']).scalar() is not None
    if not exists:
        query_logger.info('tipid did not return existing row. Creating new business instance')
        session.add(Tip(**record))
    else:
        session.query(Tip).filter_by(tipid=record['tipid']).update(record)


def make_or_update_review(session, record, *args, **kwargs):
    # Check if existing to UPDATE or INSERT
    exists = session.query(Review).filter_by(reviewid=record['reviewid']).scalar() is not None
    if not exists:
        query_logger.info('reviewid did not return existing row. Creating new business instance')
        session.add(Review(**record))
    else:
        session.query(Review).filter_by(reviewid=record['reviewid']).update(record)