# # Perceptron
#
# A reference implementation for sequenctial densely-connected layers with annotation built for flexibility.
#
# For further a single node, single layer, see a great example at:
#
# [A Perceptron in just a few lines of code](https://maviccprp.github.io/a-perceptron-in-just-a-few-lines-of-python-code/)
#
#
# This network is for experimentation purposes and seeks to align with common APIs.  I wanted a testbed for implementing common accessory algorithms to see how they affect weight updating in densely connected layers.
#
# A **formula** based approach.
#
# > network_description = {('input', X), ... ('output', (X.shape[1], num_outputs)}
#
# ```python
# example_network_description = (
#     ('input', X),  # row 0 must be input
#     ('hidden_1', (X.shape[1], 3), 'simple random'),  # hidden vectors must match input vec
#     ('hidden_2', (3, 7), 'simple random'),
#     ('output', (7,1), 'simple random'),  # final active row must be output vector. match last hidden vec
#     ('target', y) # last row in description must be the target vector
# )
# ```
#
# > neural_net = NeuralNetwork(network_description)
#
# > neural_net.fit(X,y)
#
# > neural_net.predict(observation)
#
#
# * Describe the network in dictionary format.  Making sure layers align is up to you (see notes below)
# * Mimics Scikit-Learn API (Fit, Predict).

# # Aligning Layers
#
# Layers must align for vector calculations to work.  The simple perceptron only uses dot product and dense (fully) connected layers.
#
# **Example**
#
# Layer_1 --> Layer_2 --> Layer_3
#
# > Layer_1.shape = (m, n)
#
# > Layer_2.shape == (n, o)
#
# > Layer_3.shape == (o, z)...
#
# Assumes all layers are two dimensionals.  Higher dimension perceptrons have not been tested.
#
# * m: number of rows
# * n: number of features (or feature space)
# * o: output size


# Custom implementation of flexible dense network with numpy
import pandas as pd
import numpy as np


#################################### Option Functions (initializers, evaluators, loss)


# Weight initialization generators
def gen_simple_random(shape):
    np.random.seed()
    return np.random.rand(shape[0], shape[1])

def gen_zeros(shape):
    return np.zeros((shape[0], shape[1]))


# Loss functions

### Not Implemented ###


# Activation Functions

### Not Implemented ###




###################################### Core Network

class LayerFactory():
    def __init__(self):
        return

    def initialize_weights(self, shape, algorithm):
        """
        Lookup available random functions and generate pseudo-random numbers for initial weights
        of specified shape.
        """
        available_generators = {
            'simple random': gen_simple_random,
            'zeros': gen_zeros,
        }

        return available_generators[algorithm](shape)

    def generate_layers(self, description):
        """
        Generate layers based on network description.

        ====Parameters====
        description: tuple or list object of layer descriptions ('name', shape)
        """
        layers = {}
        layers[0] = Layer()
        layers[0].activated_values = description[0][1]
        for count, row in enumerate(description):
            if row[0] == 'target':
                layers[count] = Layer()
                layers[count].activated_values = description[len(description)-1][1]

            elif row[0] != 'input':
                layers[count] = Layer()
                layers[count].weights = self.initialize_weights(shape=row[1], algorithm=row[2])
                layers[count].weighted_sum = 0
                layers[count].activated_values = 0


        return layers


# An empty class that acts like a dictionary
class Layer():
    pass


class Perceptron(LayerFactory):
    """
    Central class.  Acts like Keras compile().  Given a description, passes description to layer factory
    and returns a sequential network.
    """
    def __init__(self, description):
        # Set up Architecture of Neural Network
        self.description = description
        self.layers = self.generate_layers(description)

    def sigmoid(self, weighted_sum):
        return 1 / (1+np.exp(-weighted_sum))

    def sigmoidPrime(self, weighted_sum):
        return weighted_sum * (1 - weighted_sum)

    def feed_forward(self, X):
        """
        Calculate the NN inference using feed forward.
        aka "predict"
        """
        for i in range(1, len(self.layers)-1):
            # Weighted sum of inputs
            #  Check if first layer (required to use feed_forward method as Predict)
            if i == 1:
                self.layers[i].weighted_sum = np.dot(X, self.layers[i].weights)
                # Activated values (local outputs)
                self.layers[i].activated_values = self.sigmoid(self.layers[i].weighted_sum)
            else:
                self.layers[i].weighted_sum = np.dot(self.layers[i-1].activated_values, self.layers[i].weights)
                # Activated values (local outputs)
                self.layers[i].activated_values = self.sigmoid(self.layers[i].weighted_sum)

        return self.layers[len(self.layers)-2].activated_values

    def backward(self, X, y, net_output, learning_rate):
        """
        Backward propagate through the network
        """
        # Step 1: Calculate errors and delta shifts for each layer (backward)
        back_prop_pos = 0
        for i in range(len(self.layers)-2, 0, -1):
            # Error in local output
            #   Check if first backprop
            if back_prop_pos == 0:
                self.layers[i].error = y - net_output
                # Apply Derivative of Sigmoid to error (adjust based on slope of activation function)
                self.layers[i].delta = self.layers[i].error * self.sigmoidPrime(net_output) * learning_rate
            else:
                self.layers[i].error = self.layers[i+1].delta.dot(self.layers[i+1].weights.T)
                # Apply Derivative of Sigmoid to error (adjust based on slope of activation function)
                self.layers[i].delta = self.layers[i].error * self.sigmoidPrime(self.layers[i].activated_values)

            back_prop_pos += 1

        # Step 2: Calculate adjustments and apply to each layer (forward)
        for i in range(1, len(self.layers)-1):
            self.layers[i].weights += self.layers[i-1].activated_values.T.dot(self.layers[i].delta)

    def fit(self, X, y, learning_rate=1, epochs=1, verbose=False):
        """
        Complete forward pass and backward pass once for each epoch
        """
        for epoch in range(epochs):
            net_output = self.feed_forward(X)
            self.backward(X, y, net_output, learning_rate)
            if verbose:
                self.display_progress(epoch, X, y)

    def predict(self, X):
        return self.feed_forward(X)

    def display_progress(self, epoch, X, y):
        print('+' + '---' * 3 + f'EPOCH {epoch+1}' + '---'*3 + '+')
        print("Loss: \n", str(np.mean(np.square(y - self.feed_forward(X)))))
        print('Expected: \n{} \nPredicted: \n{}'.format(y, self.feed_forward(X)))

