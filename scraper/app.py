"""
Scraper Application

Master control module for yelp scraper
"""

import logging
import os
import time

from app_global import g

###############
### Logging ###
###############

log_path = os.path.join(os.getcwd(), 'logs', 'debug.log')
logging.basicConfig(filename=log_path)


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


###########################
### Bootstrap Functions ###
###########################

def load_categories(filename = 'categories.json'):
    categories = pd.read_json(filename)
    category_list = categories.query("country == 'US'").parent.unique().tolist()
    write_categories(category_list=category_list)
    

def bootstrap_search(center_coord: tuple):
    # Get active categories
    categories = list_categories()
    # Generate area map (model positions)
    g.modelmap = lens.ModelMap(
        center_coord = center_coord,
        map_radius=0.5,
        model_radius=0.1,
    )
    # Initialize scrapers
    g.scrapers = []
    for category in categories:
        g.scrapers.append(
            GeoScraper(
                start_coord = g.modelmap.map[0],
                radius=1,
                category=category
                )
            )   
    
    # Run on random set of model coordinates to fill map in
    for index in np.random.choice(len(g.modelmap.map), int(len(g.modelmap.map)/20), replace=False):
        print('Search at {}'.format(g.modelmap.map[index]))
        scraper_url_logger.info('Searching all categories at {}'.format(g.modelmap.map[index]))
        for scraper in g.scrapers:
            scraper.coordinates = g.modelmap.map[index]
            try:
                scraper.search()
            except:
                scraper_url_logger.error('YelpAPIError: INTERNAL_ERROR.  Skipping search.')
            time.sleep(np.random.randint(2,5))  # Yelp FusionAPI seems ok with rand(2,5) delay



if __name__ == "__main__":
    app = create_app()
    app.run()