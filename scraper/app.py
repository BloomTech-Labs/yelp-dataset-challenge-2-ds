"""
Scraper Application

Master control module for yelp scraper
"""

import logging
import os
from scraper_1_urls import search
from write_query import (filter_unique, write_business_search, write_search_metadata)

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
    def __init__(self, start_coord, radius, category):
        self.coordinates = start_coord
        self.radius = radius
        self.category = category
        self.path = self.generate_path(start_coord, radius)

    def generate_path(self, coordinates, radius):
        # Create a contiguous path that covers area given by radius.
        pass

    def run(self):
        # Loop through hops until the end of path is reached
        pass

    def get_hop(self, max=0.01):
        # Iterate through lens forward passes until sum(fastmap(hopsize, [coord])) predicts
        #   Approaches limit or max hop
        
        # Dummpy hop (default max) <- FIX WILL MOVE IN STRAIGHT LINE
        new_lat = self.coordinates(0) + max
        new_long = self.coordinates(1) + max
        self.coordinates = (new_lat, new_long)

    def search(self):
        results = search(
            category=self.category,
            latitude=self.coordinates[0],
            longitude=self.coordinates[1]
            )
        unique_results = filter_unique(results)
        self.save_search(
            unique_results=unique_results,
        )
        self.move()

    def move(self):
        pass

    def save_search(self, unique_results):
        # Save new businesses
        write_business_search(unique_frame=unique_results)
        # Save search results metadata
        search_record = {
            'latitude': self.coordinates[0],
            'longitude': self.coordinates[1],
            'category': self.category,
            'num_unique': len(unique_results),
        }
        write_search_metadata(record=search_record)


def create_scraper(city, radius, category, coordinates = None):
    # Create scraper and its Lens
    # TODO Initiate lens modelmap (need to draw map)
    if city is not None:
        coordinates = get_coordinates(city)
    elif coordinates is None:
        scraper_logger.error('City or Coordinates invalid')
        raise ValueError('City and coordinates not provided')
    
    return Scraper(
        start_coord=coordinates,
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
    print(scraper.coordinates)