from config import DATABASE_URI # get the database URI from file in gitignore
from models import * # download models.py from db_api folder
from db import get_session # download db.py from db_api folder

def biz_words(session, params, *args, **kwargs):
    response = session.query(Review.date, Review.token, Review.stars).filter(Review.business_id==params['business_id']).order_by(Review.date)
    return {'data': response.all()}

params = {'business_id' : '1SWheh84yJXfytovILXOAQ'}

with get_session() as session:
    test = biz_words(session, params)
    print(test)

reviews = test.get('data')

"""
Attributes to get for ranking
Business:
    review_count = Column(Integer)
    #TODO populate field with comparison companies
    stars = Column(Integer) 
    ##^^ Use stars if review_count doesn't match number 
    ## of reviews in db

Review:
    review_id = Column(String, primary_key=True)
    stars = Column(Float)
    business_id = Column(String, ForeignKey('businesses.business_id'))
    (maybe)     user_id = Column(String, ForeignKey('users.user_id'))

ReviewSentiment:
    review_id = Column(String, ForeignKey('reviews.review_id'))
    polarity = Column(Float)
    subjectivity = Column(Float)
    TODO how to use subjectivity?

TipSentiment:
    tip_id = Column(String, ForeignKey('tips.tip_id'))
    polarity = Column(Float)
    subjectivity = Column(Float)

Formula:

B_stars = bayesian estimate of stars
B_review_sentiment = bayesian estimate of sentiment
B_tip_sentiment = bayesian estimate of tip sentiment

ZB_stars = Z-score of bayesian estimate of stars
ZB_review_sentiment = Z-score of bayesian estimate of sentiment
ZB_tip_sentiment = Z-score of bayesian estimate of tip sentiment

C_STARS = coefficient to weight ZB_stars by
C_REVIEW_SENTIMENT = coefficient to weight ZB_review_sentiment by
C_TIP_SENTIMENT = coefficient to weight ZB_tip_sentiment by

business_Z_score = ZB_stars(C_STARS) +
                    ZB_review_sentiment(C_REVIEW_SENTIMENT) +
                    ZB_tip_sentiment(C_TIP_SENTIMENT)

business_percentile = Z_to_percentile(business_Z_score)

"""

def create_ranking(business_id):

    # Coefficients for ranking elements
    # TODO set weights
    C_STARS = .5
    C_REVIEW_SENTIMENT = .35
    C_TIP_SENTIMENT = .15
    
    # Getting info from DB
    business = get_business_data(business_id)
    comparison = get_comparison(business)
    ZB_stars = get_ZB_stars(business, comparison)
    ZB_review_sentiment = get_ZB_review_sentiment(business, comparison)
    ZB_tip_sentiment = get_ZB_tip_sentiment(business, comparison)

    business_Z_score = ((ZB_stars*C_STARS) +
                    (ZB_review_sentiment*C_REVIEW_SENTIMENT) +
                    (ZB_tip_sentiment*C_TIP_SENTIMENT))

    business_percentile = Z_to_percentile(business_Z_score)

    return business_percentile

def biz_query(session, business_id, *args, **kwargs):

    response = session.query(Review.date, Review.token, Review.stars).filter(Review.business_id==business_id).order_by(Review.date)
    return response.all()

def get_business_data(business_id):

    with get_session() as session:
        business = biz_query(session, business_id)
     
    return business

def get_comparison(business_id):
    # should already be a field populated in db with list 
    # of business_ids for comparison

    pass

def get_ZB_stars(business, comparison):
    pass

def get_ZB_review_sentiment(business, comparison):
    pass

def get_ZB_tip_sentiment(business, comparison):
    pass

def Z_to_percentile(business_Z_score):
    pass


