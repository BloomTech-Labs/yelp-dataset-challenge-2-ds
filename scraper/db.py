"""
Database
    Initialize and create connection control flow for database.
    Datase parameters must be set in config.py or directly in app.py
"""


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from models import *
from decouple import config

import logging

from app_global import g


def get_db():
    """
    Returns current database connection.  If connection not present,
    initiates connection to configured database.  Default is non-authenticated SQL.
    Modifty g.db = *connect to match intended database connection.
    """
    db_logger = logging.getLogger(__name__ + '.getdb')
    if not hasattr(g, 'db'):
        db_logger.info('DB connection not found. Attempting connection to {}.'.format(config('DATABASE_URI', default="sqlite:///scraper.db")))
        try:
            g.engine = create_engine(config('DATABASE_URI', default="sqlite:///scraper.sqlite3"))
            g.db = g.engine.connect()
        except:
            db_logger.error('Could not establish connection.  Aborting.')
            raise ConnectionError

    return g.db


@contextmanager
def get_session():
    # Setup session with thread engine.
    #   Allows for usage: with get_session() as session: session...
    engine = get_db()
    session = scoped_session(sessionmaker(bind=engine))
    try:
        yield session
    finally:
        session.close()


def close_db(e=None):
    db = get_db()
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    Base.metadata.create_all(db)
