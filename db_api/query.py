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
def assign_maker(table_name):
    makers = {
        'businesses': make_update_business,
    }
    return makers[table_name]


def make_update_business(session, record, *args, **kwargs):
    record = record
    def data_or_none(category):
        try:
            return record[category]
        except:
            return None
    # Check if existing to UPDATE or INSERT
    exists = session.query(Business).filter_by(businessid=record['businessid']).scalar() is not None
    if not exists:
        query_logger.info('businessid did not return existing row. Creating new business instance')
        session.add(
            Business(
                businessid=data_or_none('businessid'),
                name=data_or_none("name"),
                latitude=data_or_none("latitude"),
                longitude=data_or_none("longitude"),
                postalcode=data_or_none("postalcode"),
                numreviews=data_or_none("numreviews"),
                stars=data_or_none("stars"),
                isopen=data_or_none("isopen"),
                attributes=data_or_none("attributes"),
                categories=data_or_none("categories"),
                )
        )
    else:
        session.query(Business).filter_by(businessid=record['businessid']).update(record)



