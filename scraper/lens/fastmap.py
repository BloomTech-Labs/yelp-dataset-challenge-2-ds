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
import pickle


class ModelMap():
    def __init__(self):
        self.radius = 1  # TODO calculate on draw

    def __repr__(self):
        return '<ModelMap> {} Cached of {} Available'.format('NA', 'NA')

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
            'radius': self.radius,
            'file_location': file_location,
        }

    def update_model(self, X, y):
        pass

    def search_models(self, coordinates):
        pass


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

def train_network(network, X, y, num_epochs=20):
    network.fit(X, y, epochs=num_epochs)

def predict(network, X):
    return network.predict(X)

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
    X=np.array([[0,1,2], [3,4,5], [6,7,8]])
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

