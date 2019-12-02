# Yelp Database API

This is the database management system for the Yelp Dataset Challenge. All GET, PUT, POST, DELETE requests are routed through this application to the database.  Query can be done via simple search or supplied JSON schema per feature needs.

**Features** include:

* Databaes connection manager

* Caching

* CORS headers

* Root landing page (rendering this README.md)

## Usage

### Making GET Requests

**See get_requests.py** for dropin functions and examples.

> params: schema

> type: python dictionary (requests will turn to JSON object)

**EXAMPLE**:

package = {
    'schema': 'biz_words',
    'params': {
        'business_id': 'ajoqEHnCZTD8-8GqGLq9-Q'
        },
}

url = URL_OF_THIS_APPLICATION/api/data
response = requests.get(url=url, json=package)
print('Status: ', response.status_code)
print('Content: ', response.text)

### Making POST Requests

*How to make requests in python*

**See snippets.py** for helper functions to transform Pandas dataframes into correct format for request.  Some data, when scraped or available raw, does not contain a unique identifier and one must be generated.  See generate_id in snippets for a repeatable hashlib implementation.

> Params: data

> Type: python dictionary

**Example:**


import requests

import pandas as pd

from snippets import df_to_query


df = pd.read_parquet('sample_users.parquet')

package = df_to_query(df=df.head(1000), tablename='users')

batch_size = len(package['data'])


start = time.time()

request = requests.post(url='https://db-api-yelp18-staging.herokuapp.com/api/data', json=package)

print(request)

stop = time.time()

print('Batch of {} processed in {}'.format(batch_size, stop-start))



OUTPUT:

<Response [200]>

Batch of 1000 processed in 12.668339490890503


* Note: requests object will not execute until an attribute is called.  here it executes when the print statement looks for a response code.

### Initializing the Database

> flask init-db

### Updates

*Version Information*

> 2019-11-11 - 0.3 Early Release:

* Multi-thread POST functionality. RDS Connection.  GET query structure not yet implemented.

> 2019-11-06 - 0.1 Pre-Release

## Testing

Testing is currently a top to bottom python script tests.py and tests expected usage of the API.

### Running the test script

> Navigate to app.py directory and start flask server with: python app.py
> Run test scripts with: python tests.py

## Notes

### Instance Folder

When committed, this repository is set to ignore the instance/ folder via .gitignore.  In development, Flask app can read from the location, but take care to not commit config.py with any API keys or database URIs.  To get logs, create the instance/logs/debug.log file.  Otherwise, the application will throw an error that debug.log doesn't exist.  See 'logging' for more details.

### Predefined Routes

The root route path is a rendered version of the README.md file unless otherwise specified.  See app.py or the Usage section above for information on accessing functioning endpoints.

### Logging

Logging is enabled by default at the INFO level.  Logs are written to instance/logs/debug.log.  You can configure this or write to external source, console by modifying basic config.

You can change this to DEBUG by modifying the following line:

> logging.basicConfig(filename=app.config['LOGFILE'], level=logging.INFO) **logging.INFO to logging.DEBUG**

> Change default save location by modifying the filename attribute in line referenced above.  **filename=<insert_filename_here>**

> Gunicorn logging has a server specific handler. When deploying, consider: gunicorn_logger = logging.getLogger('gunicorn.error') \ app.logger.handlers = gunicorn_logger.handlers

## Deploying

Deploy only this subfolder.  If part of a larger repository, use the following:

> git subtree push --prefix db_api heroku master

#### Special Thanks

It can be hard to get eyes on a project to guide it's feature development.  I want to give special thanks to the data scientists who took a minute to walk through pain points and features they'd like to see.  Got most of it in.

* Harsh Desai
* John Morrison
* Han Lee
