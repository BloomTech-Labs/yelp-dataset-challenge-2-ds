from config import DATABASE_URI # get the database URI from file in gitignore
from models import * # download models.py from db_api folder
from db import get_session # download db.py from db_api folder
import pandas as pd

def biz_words(session, params, *args, **kwargs):
    response = session.query(Review.date, Review.token, Review.stars).filter(Review.business_id==params['business_id']).order_by(Review.date)
    return {'data': response.all()}

params = {'business_id' : '1SWheh84yJXfytovILXOAQ'}

with get_session() as session:
    test = biz_words(session, params)
    print(test)

reviews = test.get('data')

"""
Business.business_id, Business.review_count, Business.stars,
Review.review_id, Review.stars, Review.useful, Review.business_id,
ReviewSentiment.review_id, ReviewSentiment.polarity,
ReviewSentiment.subjectivity, TipSentiment.tip_id, 
TipSentiment.polarity, TipSentiment.subjectivity,
Tip.tip_id, Tip.date, Tip.compliment_count, Tip.business_id

Attributes to get for ranking
Business:
    business_id = Column(String, primary_key=True)
    review_count = Column(Integer)
    #TODO populate field with comparison companies
    stars = Column(Integer) 
    ##^^ Use stars if review_count doesn't match number 
    ## of reviews in db

Review:
    review_id = Column(String, primary_key=True)
    stars = Column(Float)
    useful = Column(Integer)
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

Tip:
    tip_id = Column(String, primary_key=True)
    date = Column(DateTime)
    compliment_count = Column(Integer)
    business_id = Column(String, ForeignKey('businesses.business_id'))

Formula:

# weights of reviews decay over time?
# useful / compliment weighting for sentiment of reviews/tips?

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
    # TODO set weights less arbitrarily
    # weight differently depending on # of reviews / tips?
    C_STARS = .5
    C_REVIEW_SENTIMENT = .35
    C_TIP_SENTIMENT = .15
    
    # Getting info from DB
    business_df, review_df, tip_df = get_business_data(business_id)
    # TODO comparison into above query once that's actually in db
    comparison = get_comparison(business_id)

    # Getting rating component Z-scores
    ZB_stars = get_ZB_stars(business, comparison)
    ZB_review_sentiment = get_ZB_review_sentiment(business, comparison)
    ZB_tip_sentiment = get_ZB_tip_sentiment(business, comparison)

    # Weighting components
    business_Z_score = ((ZB_stars*C_STARS) +
                    (ZB_review_sentiment*C_REVIEW_SENTIMENT) +
                    (ZB_tip_sentiment*C_TIP_SENTIMENT))

    # Converting ratings to percentile
    business_percentile = Z_to_percentile(business_Z_score)

    return business_percentile

def get_business_data(business_id):
    # TODO handle exceptions connecting to db
    with get_session() as session:

        # Getting info from Business table
        business_query = session.query(
            Business.business_id, \
            Business.review_count, Business.stars \
            ).filter(Business.business_id==business_id).all()
        business_df = pd.DataFrame(business_query)

        # Getting review info
        review_query = session.query(Review.review_id, Review.stars, \
            Review.useful, ReviewSentiment.review_id, Review.date, \
            ReviewSentiment.polarity, ReviewSentiment.subjectivity \
            ).join(ReviewSentiment).filter(Review.business_id==business_id).all()
        review_df = pd.DataFrame(review_query)

        # Getting tip info
        tip_query = session.query(Tip.date, \
            Tip.compliment_count, Tip.business_id, \
            TipSentiment.tip_id, TipSentiment.polarity, \
            TipSentiment.subjectivity \
            ).join(TipSentiment).filter(Tip.business_id==business_id).all()
        tip_df = pd.DataFrame(tip_query)

    return business_df, review_df, tip_df

def get_comparison(business_id):
    # should already be a field populated in db with list 
    # of business_ids for comparison
    # TODO change from test list

    PLACEHOLDER_LIST = [
       'ujmEBvifdJM6h6RLv4wQIg',
        'NZnhc2sEQy3RmzKTZnqtwQ',
        'WTqjgwHlXbSFevF32_DJVw',
        'ikCg8xy5JIg_NGPx-MSIDA',
        'b1b1eb3uo-w561D0ZfCEiQ',
        'eU_713ec6fTGNO4BegRaww',
        '3fw2X5bZYeW9xCz_zGhOHg',
        'zvO-PJCpNk4fgAVUnExYAA',
        'b2jN2mm9Wf3RcrZCgfo1cg',
        'oxwGyA17NL6c5t1Etg5WgQ'
    ]

    return PLACEHOLDER_LIST 

def get_ZB_stars(business, comparison):
    pass

def get_ZB_review_sentiment(business, comparison):
    pass

def get_ZB_tip_sentiment(business, comparison):
    pass

def Z_to_percentile(business_Z_score):
    pass



### Scratchpaper
with get_session() as session:
    test = session.query(Review.review_id, Review.stars, ReviewSentiment.polarity, ReviewSentiment.subjectivity).join(ReviewSentiment).filter(Review.business_id==business_id).all()

business_id = '1SWheh84yJXfytovILXOAQ'