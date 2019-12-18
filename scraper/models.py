"""
Model mapping for scraper.

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
    url = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    postal_code = Column(String)
    review_count = Column(Integer)
    stars = Column(Integer)
    is_open = Column(Integer)
    categories = Column(String)
    image_url = Column(String)


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


class User(Base):
    __tablename__ = 'users'

    user_id = Column(String, primary_key=True)


class Perceptron(Base):
    __tablename__ = 'perceptrons'

    geohash = Column(String, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    radius = Column(Float)
    observations = Column(Integer)
    file_location = Column(String)


class SearchResults(Base):
    __tablename__ = 'search_results'

    search_num = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    category = Column(String)
    num_unique = Column(Integer)
