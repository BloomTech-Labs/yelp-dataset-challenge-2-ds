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

    businessid = Column(String, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    postalcode = Column(Integer)
    numreviews = Column(Integer)
    stars = Column(Integer)
    isopen = Column(Binary)
    attributes = Column(String)
    categories = Column(String)


class User(Base):
    __tablename__ = 'users'

    userid = Column(String, primary_key=True)
    name = Column(String)
    reviewcount = Column(Integer)
    averagestars = Column(Float)
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

    checkinid = Column(String, primary_key=True)
    datetime = Column(DateTime)
    businessid = Column(String, ForeignKey('businesses.businessid'))


class Photo(Base):
    __tablename__ = 'photos'

    photoid = Column(String, primary_key=True)
    caption = Column(String)
    label = Column(String)
    businessid = Column(String, ForeignKey('businesses.businessid'))


class Tip(Base):
    __tablename__ = 'tips'

    tipid = Column(String, primary_key=True)
    compliment = Column(Integer)
    datetime = Column(DateTime)
    text = Column(Text)
    token = Column(Text)
    tokenvector = Column(Text)
    ngram = Column(Text)
    businessid = Column(String, ForeignKey('businesses.businessid'))
    userid = Column(String, ForeignKey('users.userid'))


class Review(Base):
    __tablename__ = 'reviews'

    reviewid = Column(String, primary_key=True)
    datetime = Column(DateTime)
    cool = Column(Integer)
    funny = Column(Integer)
    stars = Column(Float)
    text = Column(Text)
    tokenvector = Column(Text)
    token = Column(Text)
    ngram = Column(Text)
    businessid = Column(String, ForeignKey('businesses.businessid'))
    userid = Column(String, ForeignKey('users.userid'))