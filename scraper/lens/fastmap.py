"""
Fast Map

Create local perceptron models for defined dataset.
Track and retrain models.
Route requests for inference, maintaining models in cache.
"""

import sqlite3
from .perceptron import Perceptron
from .geohash import decode, encode
import numpy as np
from math import ceil
import pickle

import logging

mm_logger = logging.getLogger(__name__)


class ModelMap():
    def __init__(self, center_coord, map_radius=1, model_radius=0.05):
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

    def __repr__(self):
        return '<ModelMap> {} Sized Grid, {} Available'.format(
            len(self.map), len(self.cache.keys()))

    def pin_model(self, X, y, coordinates):
        """
        Create model, train, and save.
        Then, save model and return record for database save
        """
        model = create_network(
            network_description=get_network_description(X, y)
        )
        train_network(model, X, y)
        hashed_coord = encode(coordinates[0], coordinates[1])
        file_location = save_model(object_to_save=model, savename=hashed_coord)
        return {
            'geohash': hashed_coord,
            'latitude': coordinates[0],
            'longitude': coordinates[1],
            'observations': len(X),
            'file_location': file_location,
        }

    def predict(self, coordinates, bootstrap=10, **kwargs):
        # Check if model is pinned at given coordinates
        model = self.search_models(coordinates)
        if model:
            X, y = sample_data(coordinates, self.model_radius)
            if model['observations'] < X:
                self.update_model(model['network'], X, y)
            return model['network'].predict(X)
        pass

    def clean_cache(self, threshold):
        # Remove element from cache if not in use to prevent memory explosion
        pass

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
    for latitude in latitudes:
        rows += list(zip(longitudes, [latitude]*len(longitudes)))
        
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

def get_network_description(X, y):
    network_description = (
        ('input', X),  # row 0 must be input
        ('hidden_1', (X.shape[1], 4), 'simple random'),  # hidden vectors must match input vec
        ('output', (4, 3), 'simple random'),  # Reduction of longitude/lattitude
        ('target', y) # last row in description must be the target vector
    )
    return network_description

def create_network(network_description):
    neural_net = Perceptron(network_description)
    return neural_net

def train_network(model, X, y, num_epochs=2000):
    mm_logger.info('Training network at latitude: {}, longitude: {}'.\
        format(model['latitude'], model['longitude']))
    model['network'].fit(X, y, epochs=num_epochs)

def save_model(object_to_save, savename, root_path='/tmp/'):
    filepath = root_path+savename+'.pkl'
    with open(filepath, 'wb') as file:
        pickle.dump(object_to_save, file)
    return filepath

def load_model(path_to_file):
    with open(path_to_file, 'rb') as file:
        model = pickle.load(file)
    return model





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

