"""
Lens: Pipeline
    Contains transformation, scaling, model creation, pipeline.
"""

from  read_query import list_categories
from app_global import g
import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler


######################
### Build Pipeline ###
######################

def build_pipeline():
    lregr = LogisticRegression(
        random_state=420,
        solver='liblinear'
        )
        
    standard_scaler = StandardScaler()

    tpipe = Pipeline([
        ('scaler', standard_scaler),
        ('logistic', lregr),
        ])

    return tpipe


########################
### Helper Functions ###
########################

def get_categories():
    if not hasattr(g, 'categories'):    
        g.categories = list_categories(with_id=True)
    return g.categories


def encode_cat(x):
    categories = get_categories()
    temp_arr = np.zeros(len(categories.keys()))
    temp_arr[categories[x]-1] = 1
    return temp_arr


def transform_observation(obs):
    assert len(obs) == 3
    return np.concatenate(
        (
            [obs[0]], [obs[1]], encode_cat(obs[2])
        ),
        axis=None
    )


def truncate_x(obs):
    return obs[0:3]


def truncate_y(obs):
    return obs[-1]


def split_widen_data(data):
    """
    Split Widen Data
        Implement custon one-hot encoding.  Splits data into shaped segments
        expected by model.  

        :param data: Data array set, either training set or observation
        :type data: np.array or array-like
    """
    X_raw = list(map(
            truncate_x, data
        ))
    X = np.array(list(map(
            transform_observation, X_raw
        )), dtype='f')
    y = np.array(list(map(
            truncate_y, data
        )), dtype='f')#.reshape(-1,1)
    return X, y