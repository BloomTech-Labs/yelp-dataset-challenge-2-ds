#!/usr/bin/env python
# coding: utf-8

# # Loss Functions
# 
# This is a library of standard loss functions.  I implemented these for use in perceptron networks and to better understand how they are calculated.
# 
# **General Use** for API (reminder on how to build for me):
# 
# * All loss functions should take in an observed vector (y_observed) and prediction vector (y_pred)

# In[4]:


# Libraries Import
import numpy as np

from errors import *


# ## Mean Squared Error (MSE)
# 
# $$\frac{\sum_{i=0}^n {(x_0 - x)}^2}{n}$$

# In[23]:

def default(*args, **kwargs):
    return 1

def mse(y_pred:np.array, y_observed:np.array, absolute_values=False):
    # Check length of prediction and observation match
    assert len(y_pred) == len(y_observed),         "Shape of Prediction and observed valeus do not match: %r %r" % (y_pred.shape, y_observed.shape)
    
    error = absolute_error(y_pred=y_pred, y_observed=y_observed, absolute_values=False)
    return sum(error**2)/len(y_observed)
    

def mse_derivative(y_pred:np.array, y_observed:np.array):
    error = absolute_error(y_pred=y_pred, y_observed=y_observed, absolute_values=False)
    return np.mean(-2 * error)

    
def ridge():
    pass


# In[ ]:





# In[2]:


def lasso():
    pass


# In[ ]:




