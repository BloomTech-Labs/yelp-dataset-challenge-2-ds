# Yelp Database API

This is the database management system for the Yelp Dataset Challenge. All GET, PUT, POST, DELETE requests are routed through this application to the database.  Query can be done via simple search or supplied JSON schema per feature needs.

**Features** include:

* Databaes connection manager

* Caching

* CORS headers

* Root landing page (rendering this README.md)

## Usage

### Making Requests

*How to make requests*

> Params, Returns

### Initializing the Database

> flask init-db

### Updates

*Version Information*

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


## Deploying

Deploy only this subfolder.  If part of a larger repository, use the following:

> git subtree push --prefix db_api heroku master

#### Special Thanks

It can be hard to get eyes on a project to guide it's feature development.  I want to give special thanks to the data scientists who took a minute to walk through pain points and features they'd like to see.  Got most of it in.

* Harsh Desai
* John Morrison
* Han Lee
