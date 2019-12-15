"""SQLAlchemy models for WordCloudApp"""

from flask_sqlalchemy import SQLAlchemy

DB = SQLAlchemy()

class reviews(DB.Model):
    __tablename__ = 'reviews'
    review_id = DB.Column(DB.String, primary_key=True)
    date = DB.Column(DB.DateTime)
    cool = DB.Column(DB.Integer)
    funny = DB.Column(DB.Integer)
    useful = DB.Column(DB.Integer)
    stars = DB.Column(DB.Float)
    text = DB.Column(DB.Text)
    token_vector = DB.Column(DB.Text)
    token = DB.Column(DB.Text)
    ngram = DB.Column(DB.Text)
    noun_chunk = DB.Column(DB.Text)
    lemma = DB.Column(DB.Text)
    business_id = DB.Column(DB.String)
    user_id = DB.Column(DB.String)
    
    def __repr__(self):
        return '<Review {}>'.format(self.review_id)

class Viz2(DB.Model):
    __tablename__ = 'viz2'

    vz_id = DB.Column(DB.Integer, primary_key=True)
    business_id = DB.Column(DB.String)
    categories = DB.Column(DB.String)
    percentile = DB.Column(DB.Float)
    competitors = DB.Column(DB.Text)
    bestinsector = DB.Column(DB.Text)
    avg_stars_over_time = DB.Column(DB.Text)
    chunk_sentiment = DB.Column(DB.Text)
    count_by_star = DB.Column(DB.Text)
    review_by_year = DB.Column(DB.Text)

    @property
    def serialize(self):
        return {'categories': self.categories, \
            'percentile': self.percentile, \
            'competitors': self.competitors, \
            'bestinsector': self.bestinsector, \
            'avg_stars_over_time': self.avg_stars_over_time, \
            'chunk_sentiment': self.chunk_sentiment, \
            'count_by_star': self.count_by_star, \
            'review_by_year': self.review_by_year}
    
    def __repr__(self):
        return '<VizRecord {}>'.format(self.vz_id)

class Business(DB.Model):
    __tablename__ = 'businesses'

    business_id = DB.Column(DB.String, primary_key=True)
    name = DB.Column(DB.String)
    address = DB.Column(DB.String)
    city = DB.Column(DB.String)
    state = DB.Column(DB.String)
    latitude = DB.Column(DB.Float)
    longitude = DB.Column(DB.Float)
    postal_code = DB.Column(DB.String)
    review_count = DB.Column(DB.Integer)
    stars = DB.Column(DB.Integer)
    is_open = DB.Column(DB.Integer)
    hours = DB.Column(DB.String)
    attributes = DB.Column(DB.String)
    categories = DB.Column(DB.String)

    @property
    def serialize(self):
        return {'name': self.name, \
            'address': self.address, \
            'city': self.city, \
            'state': self.state, \
            'review_count': self.review_count, \
            'stars': self.stars}

    def __repr__(self):
        return '<Business {}>'.format(self.business_id)