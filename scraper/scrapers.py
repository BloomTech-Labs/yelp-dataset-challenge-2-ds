"""
Scrapers
    Scraper class objects to wrap functionality
"""
import numpy as np
from write_query import (filter_unique, write_business_search, write_search_metadata,
                            write_categories)
import lens
from app_global import g

import logging
scraper_logger = logging.getLogger(__name__)


class Scraper():
    def __init__(self):
        self.logger = scraper_logger
        self.logger.info("Creating Scraper")
        self.working_item = None
        self.complete = []
        self.failed = []
    def run(self):
        NotImplemented
    def move(self):
        NotImplemented
    def search(self):
        NotImplemented
    def save(self):
        NotImplemented


class GeoScraper(Scraper):
    """
    GeoScraper
        Geographically aware scraper implementing Lens for smart pathing
    """
    def __init__(self, start_coord, radius, category):
        self.coordinates = start_coord
        self.max_radius = radius
        self.category = category
        # Formula terms
        self.coord_polar = {'r':0, 'theta':0, 'a':calc_a_max(radius)}
        super().__init__()

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
        results = geo_search(
            category=self.category,
            latitude=self.coordinates[0],
            longitude=self.coordinates[1]
            )
        unique_results = filter_unique(results)
        self.save(
            unique_results=unique_results,
        )
        # self.move()

    def save(self, unique_results):
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


class ListScraper(Scraper):
    """
    ListScraper
        Natively multithreaded scraper to return multiple requests given a
        list of search strings and appropriate search function.
    """
    def __init__(self, search_list: list):
        self.search_list = search_list
        super().__init__()

    def search(self):
        # query URL in working. if save successful
        pass

    def move():
        # pop url, store popped temporarily in working.
        pass


########################
### Helper Functions ###
########################

# GeoScraper Functions #

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


def create_geo_scraper(city, radius, category, coordinates = None):
    """
    Create a geographic yelp scraper for businesses. 
    """

    if city is not None:
        coordinates = lookup_city_coordinates(city)
    elif coordinates is None:
        scraper_logger.error('City or Coordinates invalid')
        raise ValueError('City and coordinates not provided')
    
    return GeoScraper(
        start_coord=coordinates,
        radius=radius,
        category=category
    )


def geo_search(category, latitude, longitude):
    # Get client and run search
    client = get_client()
    search_results = client.search_query(
        categories=category, latitude=latitude, longitude=longitude, limit=50
        )
    df = pd.DataFrame(search_results['businesses'])
    return clean_business_search(df)


# ListScraper Functions #

