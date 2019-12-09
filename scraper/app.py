"""
Scraper Application

Master control module for yelp scraper
"""

import logging
import os

###############
### Logging ###
###############

log_path = os.path.join(os.getcwd(), 'logs', 'debug.log')
logging.basicConfig(filename=log_path)
scraper_logger = logging.getLogger(__name__)


###########################
### Application Factory ###
###########################

class Scraper():
    def __init__(self, start, radius, category):
        self.start = start
        self.radius = radius
        self.category = category

    def run(self):
        pass



def create_scraper(city, radius, category, coordinates = None):
    # Create scraper and its Lens
    # TODO Initiate lens modelmap (need to draw map)
    if city is not None:
        coordinates = get_coordinates(city)
    elif coordinates is None:
        scraper_logger.error('City or Coordinates invalid')
        raise ValueError('City and coordinates not provided')
    
    return Scraper(
        start=coordinates,
        radius=radius,
        category=category
    )


########################
### Helper Functions ###
########################

def get_coordinates(city):
    # INCOMPLETE
    # TODO city lookup
    return (0,0)
    


if __name__ == "__main__":
    scraper = create_scraper(city='test', radius=10, category='test')
    print(scraper.start)