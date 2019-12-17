"""
Scraper Application

Master control module for yelp scraper
"""

import logging
import os
import numpy as np
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
        self.max_radius = radius
        self.category = category
        # Formula terms
        self.coord_polar = {'r':0, 'theta':0, 'a':calc_a_max(radius)}

    def run(self):
        # Loop through hops until the end of path is reached
        pass

    def move(self, d_theta=np.pi/12, c=0.025):
        # Calculate expected value for d_theta and adjust curvature
        def delta_a(a, expected_value, c):
            return c * a * (50-expected_value)/expected_value
        
        expected_value = predict_capture()
        a = self.coord_polar['a']
        self.coord_polar['a'] = a + delta_a(a, c, expected_value)


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
        coordinates = lookup_city_coordinates(city)
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

def calc_a_max(max_radius):
    a_max = [np.log(max_radius) / theta for theta in np.linspace(0.001, 2*np.pi, 12)]
    a_max = min(a_max)/10
    return a_max


def get_decimal_from_polar(center_coord, r, theta, a):
    def get_offset(theta, a):
        r = np.exp(-a*theta)
        lat = r * np.sin(theta)
        lon = r * np.cos(theta)
        return(lat,lon)
    offset = get_offset(theta, a)
    return (center_coord[0]+offset[0], center_coord[1]+offset[1])


def lookup_city_coordinates(city):
    # INCOMPLETE
    # TODO city lookup
    return (0,0)


def predict_capture():
    pass


def sample_data(coordinates):
    pass



if __name__ == "__main__":
    scraper = create_scraper(city='test', radius=10, category='test')
    print(scraper.coordinates)