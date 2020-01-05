"""
Scraper Application

Master control module for yelp scraper
"""

import logging
import os
import numpy as np
import pandas as pd
import time

from app_global import g

###############
### Logging ###
###############

log_path = os.path.join(os.getcwd(), 'logs', 'debug.log')
logging.basicConfig(filename=log_path)
scraper_logger = logging.getLogger(__name__)


###########################
### Application Factory ###
###########################

class App():
    def __init__(self):
        pass
    def run(self):
        pass

def create_app():
    app = App()
    return app


########################
### Helper Functions ###
########################




def lookup_city_coordinates(city):
    # INCOMPLETE
    # TODO city lookup
    return (0,0)



if __name__ == "__main__":
    app = create_app()
    app.run()