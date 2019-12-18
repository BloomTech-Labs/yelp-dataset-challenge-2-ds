#!/usr/bin/env python
# coding: utf-8

# # Error Functions
# 
# This is a library of standard error functions. I implemented these for use in perceptron networks and to better understand how they are calculated.
# 
# **General Use** for API (reminder on how to build for me):
# 
# * All error functions should take in an observed vector (y_observed) and prediction vector (y_pred)

# In[1]:


# Library Imports
import numpy as np


# ## Absolute Error
# 
# $${y_0 - y}$$

# In[19]:


def absolute_error(y_pred:np.array, y_observed:np.array, absolute_values=False):
    """
    Calculates vector errors.
    """
    if absolute_values:
        return abs(y_observed - y_pred)
    return y_observed - y_pred



