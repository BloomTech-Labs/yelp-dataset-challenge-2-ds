"""
Fast Map

Create local perceptron models for defined dataset.
Track and retrain models.
Route requests for inference, maintaining models in cache.
"""

from .geohash import decode, encode
from .pipeline import build_pipeline
import numpy as np
from math import ceil
import pickle
import time

import logging

mm_logger = logging.getLogger(__name__)


class ModelMap():
    def __init__(self, center_coord, map_radius=1, model_radius=0.05, max_cache=10):
        self.radius = None  # Not Implemented (part of search_model)
        self.map = get_grid_coord(
            c_lat = center_coord[0],
            c_lon = center_coord[1],
            point_radius=model_radius,
            max_radius=map_radius
        )
        self.center_coord = center_coord
        self.map_radius = map_radius
        self.model_radius = model_radius
        self.cache = {}
        self.max_cache = max_cache

    def __repr__(self):
        return '<ModelMap> {} Sized Grid, {} Available'.format(
            len(self.map), len(self.cache.keys()))

    def pin_model(self, X, y, coordinates):
        """
        Create model, train, and save.
        Then, save model and return record for database save
        """
        model = create_network()
        train_network(model=model, X=X, y=y, coordinates=coordinates)
        geohash = encode(coordinates[0], coordinates[1])
        file_location = save_model(object_to_save=model, savename=geohash)
        self.cache_model(geohash=geohash, model=model)
        return {
            'geohash': geohash,
            'latitude': coordinates[0],
            'longitude': coordinates[1],
            'radius': self.model_radius,
            'observations': len(X),
            'file_location': file_location,
        }

    def predict(self, coordinates, **kwargs):
        # Check if model is pinned at given coordinates
        model = self.search_models(coordinates)
        if model:
            X, y = sample_data(coordinates, self.model_radius)
            if model['observations'] < X:
                self.update_model(model['network'], X, y)
            return model['network'].predict(X)
        pass

    def cache_model(self, geohash, model):
        self.cache.update(
            {time.time():{'model': model,
                      'geohash': geohash
                      }
            }
        )
        self.clean_cache()

    def clean_cache(self):
        # Check current cache size and pop oldest elements
        cache_len = len(self.cache.keys())
        if cache_len > self.max_cache:
            for _ in range(cache_len - self.max_cache):
                self.cache.pop(get_oldest_cached(self.cache))


    def update_model(self, model, X, y):
        # Pass in model object including all information from the database and 
        #   loaded model as dict
        train_network(
            model=model,
            X=X,
            y=y
        )

    def search_models(self, coordinates, limit=0.01):
        # TODO Lookup which model is most appropriate for search
        # If no model within limit of coordinates, pin new model
        return False


####################
### Generate Map ###
####################

def get_grid_coord(c_lat, c_lon, point_radius, max_radius):
    latitudes = generate_row(center=c_lat, point_radius=point_radius, max_radius=max_radius)
    longitudes = generate_row(center=c_lon, point_radius=point_radius, max_radius=max_radius)
    
    rows = []
    for longitude in longitudes:
        rows += list(zip(latitudes, [longitude]*len(latitudes)))
        
    return rows


def calc_nodes_per_row(point_radius, max_radius):
    X = 2 * max_radius # X = Y; square matrix
    num_nodes_per_row = int(ceil(X/point_radius)) # no partial nodes, must be odd number
    if num_nodes_per_row % 2 == 0:
        num_nodes_per_row += 1
    return num_nodes_per_row


def calc_distance_between_nodes(num_nodes, max_radius, scale_factor=1):
    return max_radius/(num_nodes-1) * scale_factor


def generate_row(center, point_radius, max_radius):
    num_nodes = calc_nodes_per_row(point_radius=point_radius, max_radius=max_radius)
    
    # Validate that point-radius > dist
    dist = calc_distance_between_nodes(num_nodes=num_nodes, max_radius=max_radius)
    assert point_radius > dist
    
    left = center - max_radius/2
    right = center + max_radius/2
    row = np.linspace(left, right, num_nodes)
    # Validate that longitude_vector same length = num_nodes
    assert len(row) == num_nodes
    return row


#####################################
### Model Generation and Handling ###
#####################################

def create_network(*args, **kwargs):
    network = build_pipeline()
    return network

def train_network(model, X, y, **kwargs):
    try:
        mm_logger.info('Training network at {}'.format(kwargs['coordinates']))
    except:
        mm_logger.info('Training network at unknown coordinates')
    model.fit(X, y)

def save_model(object_to_save, savename, root_path='/tmp/'):
    filepath = root_path+savename+'.pkl'
    with open(filepath, 'wb') as file:
        pickle.dump(object_to_save, file)
    return filepath

def load_model(path_to_file):
    with open(path_to_file, 'rb') as file:
        model = pickle.load(file)
    return model


def transform_input(input_array):
    pass


###############################
### Helpful Logic Functions ###
###############################

def get_oldest_cached(cache):
    """ Iterate through items in cache and get return the key of the oldest item
            The cache is designed such that the keys are instantiation times.
    """
    oldest = min(cache.keys())
    return oldest



if __name__ == "__main__":
    # Test with n observations of each category.
    # In practice, these will be imputed values or values from nearby locations
    X=np.array([[0,1,2], [3,2,5], [6,7,8], [9, 10, 11]])
    y=np.array([0.5,0.65,1])
    description = get_network_description(
        X=X,
        y=y
    )
    netA = create_network(network_description=description)

    train_network(netA, X, y)

    print("averaging values from final nodes")
    print(np.mean(netA.feed_forward(X), axis=0))

    print("pickling test model")
    with open('/tmp/netA.pkl', 'wb') as file:
        pickle.dump(netA, file)

    print("loading model")
    with open('/tmp/netA.pkl', 'rb') as file:
        netB = pickle.load(file)

    print('Forward pass with loaded model')
    print(netB.feed_forward(X))

