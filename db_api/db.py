"""
Database
    Initialize and create connection control flow for database.
    Datase parameters must be set in config.py or directly in app.py
"""


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from models import *

import click
from flask import current_app, g
from flask.cli import with_appcontext

import logging


def get_db():
    """
    Returns current database connection.  If connection not present,
    initiates connection to configured database.  Default is non-authenticated SQL.
    Modifty g.db = *connect to match intended database connection.
    """
    db_logger = logging.getLogger(__name__ + '.getdb')
    if 'db' not in g:
        db_logger.info('DB connection not found. Attempting connection to {}.'.format(current_app.config['DATABASE_URI']))
        try:
            engine = create_engine(current_app.config['DATABASE_URI'])
            g.db = engine.connect()
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
    db = g.pop('db', None)

    if db is not None:
        db.close()
        db.dispose()


def init_db():
    db = get_db()
    Base.metadata.create_all(db)

@click.command('init-db')
@with_appcontext
def init_db_command():
    """Create tables from models.py"""
    init_db()
    click.echo('Initialized the database')


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
