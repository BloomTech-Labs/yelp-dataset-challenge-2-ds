"""
Model mapping for db_api

Setup notes:
https://docs.sqlalchemy.org/en/13/orm/tutorial.html
"""
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy import \
    (Column, Integer, String, ForeignKey, DateTime, Float, Binary, Text)
from sqlalchemy.orm import relationship

###Data Models###
class Business(Base):
    __tablename__ = 'businesses'

    business_id = Column(String, primary_key=True)
    name = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    postal_code = Column(String)
    review_count = Column(Integer)
    stars = Column(Integer)
    is_open = Column(Integer)
    hours = Column(String)
    attributes = Column(String)
    categories = Column(String)


class User(Base):
    __tablename__ = 'users'

    user_id = Column(String, primary_key=True)
    name = Column(String)
    review_count = Column(Integer)
    average_stars = Column(Float)
    weight = Column(Float)
    friends = Column(Text)
    yelping_since = Column(DateTime)
    compliment_cool = Column(Integer)
    compliment_cute = Column(Integer)
    compliment_funny = Column(Integer)
    compliment_hot = Column(Integer)
    compliment_list = Column(Integer)
    compliment_more = Column(Integer)
    compliment_note = Column(Integer)
    compliment_photos = Column(Integer)
    compliment_plain = Column(Integer)
    compliment_profile = Column(Integer)
    compliment_writer = Column(Integer)
    cool = Column(Integer)
    elite = Column(String)
    fans = Column(Integer)
    funny = Column(Integer)
    useful = Column(Integer)


class Checkin(Base):
    __tablename__ = 'checkins'

    checkin_id = Column(String, primary_key=True)
    dates = Column(Text)
    business_id = Column(String, ForeignKey('businesses.business_id'))


class Photo(Base):
    __tablename__ = 'photos'

    photo_id = Column(String, primary_key=True)
    caption = Column(String)
    label = Column(String)
    business_id = Column(String, ForeignKey('businesses.business_id'))


class Tip(Base):
    __tablename__ = 'tips'

    tip_id = Column(String, primary_key=True)
    compliment_count = Column(Integer)
    date = Column(DateTime)
    text = Column(Text)
    token = Column(Text)
    token_vector = Column(Text)
    ngram = Column(Text)
    noun_chunk = Column(Text)
    lemma = Column(Text)
    business_id = Column(String, ForeignKey('businesses.business_id'))
    user_id = Column(String, ForeignKey('users.user_id'))


class Review(Base):
    __tablename__ = 'reviews'

    review_id = Column(String, primary_key=True)
    date = Column(DateTime)
    cool = Column(Integer)
    funny = Column(Integer)
    useful = Column(Integer)
    stars = Column(Float)
    text = Column(Text)
    token_vector = Column(Text)
    token = Column(Text)
    ngram = Column(Text)
    noun_chunk = Column(Text)
    lemma = Column(Text)
    business_id = Column(String, ForeignKey('businesses.business_id'))
    user_id = Column(String, ForeignKey('users.user_id'))


class ReviewSentiment(Base):
    __tablename__ = 'review_sentiment'

    rs_id = Column(Integer, primary_key=True)
    review_id = Column(String, ForeignKey('reviews.review_id'))
    polarity = Column(Float)
    subjectivity = Column(Float)


class TipSentiment(Base):
    __tablename__ = 'tip_sentiment'

    ts_id = Column(Integer, primary_key=True)
    tip_id = Column(String, ForeignKey('tips.tip_id'))
    polarity = Column(Float)
    subjectivity = Column(Float)


class Viz2(Base):
    __tablename__ = 'viz2'

    vz_id = Column(Integer, primary_key=True)
    business_id = Column(String, ForeignKey('businesses.business_id'))
    categories = Column(String)
    percentile = Column(Float)
    competitors = Column(Text)
    bestinsector = Column(Text)
    avg_stars_over_time = Column(Text)
    chunk_sentiment = Column(Text)
    count_by_star = Column(Text)
    review_by_year = Column(Text)