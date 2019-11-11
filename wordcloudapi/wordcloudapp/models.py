"""SQLAlchemy models for WordCloudApp"""

from flask_sqlalchemy import SQLAlchemy

DB = SQLAlchemy()

class reviews(DB.Model):
    __tablename__ = 'reviews'
    business_id = DB.Column(DB.String)
    cool = DB.Column(DB.Integer)
    date_time = DB.Column(DB.DateTime)
    funny = DB.Column(DB.Numeric)
    review_id = DB.Column(DB.String, primary_key=True)
    star_review = DB.Column(DB.Numeric)
    text = DB.Column(DB.Text)
    useful = DB.Column(DB.Numeric)
    user_id = DB.Column(DB.String)
    tokens = DB.Column(DB.Text)
    date = DB.Column(DB.DateTime)
    
    def __repr__(self):
        return '<Business {}>'.format(self.business_id)