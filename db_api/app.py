from flask import Flask, request, jsonify
from flask import current_app, g
from flask_cors import CORS
from flask_caching import Cache
from decouple import config
from markdown2 import Markdown
import os
import sys

# Custom errors
from errors import InvalidUsage

# Logging
import logging

###########
###Setup###
###########
# Local Environment Testing Only.
#   Un-comment to build enviorment script in instance folder.
# from instance import setup
# setup.setup_env()

# Set database name
local_db_name = 'test.sqlite3'  # Change this or override with config.py file in instance/

#########################
###Application Factory###
#########################

# Create the application instance by calling create_app()
#     Example: app = create_app()

def create_app(test_config=None):
    # Create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # If environment vairables not set, will default to development expected paths and names
    app.config.from_mapping(
        DEBUG=config('DEBUG', default=False),  # Make sure to change debug to False in production env
        SECRET_KEY=config('SECRET_KEY', default='dev'),  # CHANGE THIS!!!!
        DATABASE_URI=config('DATABASE_URI', 'sqlite:///' + os.path.join(os.getcwd(), local_db_name)),  # For in-memory db: default='sqlite:///:memory:'),
        LOGFILE=config('LOGFILE', os.path.join(app.instance_path, 'logs/debug.log')),
        CACHE_TYPE=config('CACHE_TYPE', 'simple'),  # Configure caching
        CACHE_DEFAULT_TIMEOUT=config('CACHE_DEFAULT_TIMEOUT', 300), # Long cache times probably ok for ML api
    )
    # Enable CORS extensions
    CORS(app)
    # Enable caching
    cache = Cache(app)

    #  Register database functions.  Will allow db.close() to run on instance teardown.
    import db
    db.init_app(app)

    #  Bring in query methods
    import query

    ############
    ###Routes###
    ############
    @app.route('/')
    @cache.cached(timeout=2)  # Agressive cache timeout.
    def root():
        # Check if README.md exists and render as HTML if present
        if os.path.isfile('README.md'):
            return render_markdown('README.md')
        return "README.md Not Found.  This is API Main.  Use */api/predict/"

    @app.route('/api/data', methods=['GET', 'POST'])
    # @cache.cached(timeout=10)  # Agressive cache timeout.  DEBUG remove caching to see about repear requests
    def data():
        # Parse request
        app_logger.info('Data request received.  Processing.')
        if request.method == 'GET':
            if not request.json:
                raise InvalidUsage(message="Search query not provided")
            # Pass json portion of request to database query handler
            search_request = request.json
            search_response = query.query_database(method='GET', query=search_request)
        elif request.method == 'POST':
            if not request.json:
                raise InvalidUsage(message="Post JSON data not provided")
            # Pass json portion of request to database query handler
            app_logger.info('POST Request recognized.  Sending to query handler.')
            search_request = request.json
            search_response = query.query_database(method='POST', query=search_request)
        else:
            raise InvalidUsage(message="Incorrect request type")

        return search_response

    #############
    ###Logging###
    #############
    # Change logging.INFO to logging.DEBUG to get full logs.  Will be a crapload of information.
    # May significantly impair performance if writing logfile to disk (or network drive).
    # To enable different services, see README.md
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    # logging.basicConfig(filename=app.config['LOGFILE'], level=logging.INFO)  # File logging
    logging.getLogger('flask_cors').level = logging.INFO
    app_logger = logging.getLogger(__name__)

    ############################
    ###Register Error Handles###
    ############################
    @app.errorhandler(InvalidUsage)
    def handle_invalid_usage(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    return app


def render_markdown(filename):
    # Convert markdown file to HTML for rendering
    with open(filename, 'rb') as f:
        html = Markdown().convert(f.read())

    return html


app = create_app()


if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    #  Run app.py.  Comment below out for Docker.
    port = int(os.environ.get('PORT', 5050))
    app.run(host='0.0.0.0', port=port)