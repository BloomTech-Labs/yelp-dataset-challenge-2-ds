"""
Path Classes

    Expose consistent movement and path sampling, jumping, analysis for 
    various classes.
"""

import numpy as np 
from functools import partial

class Path():
    def __init__(self, step_size, start_coord):
        self.step_size = step_size
        self.path_len = self.calc_path_len()
        self.start_coord = start_coord

    def move(self):
        NotImplemented

    def sample(self, **kwargs):
        NotImplemented

    def calc_path_len(self):
        raise NotImplementedError

    def convert_latlong(self):
        NotImplemented

    def get_coord(self):
        NotImplemented


class SpiralPath(Path):
    """
    Logistic Sprial Path
    r = e^(-a*theta)
    """
    def __init__(self, center_coord=[0,0], step_size=np.pi/12, a=0.025, max_radius=1):
        self.theta = 0
        self.a = a
        self.center_coord = center_coord
        self.max_radius = max_radius
        self.coord = self.convert_latlong(theta=self.theta, a=a)
        
        super().__init__(step_size=step_size, start_coord=center_coord)

    def move(self, d_theta=None, c=0.025, step=1, **kwargs):
        magnitude = kwargs['magnitude']
        # Calculate expected value for d_theta and adjust curvature
        def delta_a(a, c, magnitude):
            return (c * a  * (1-magnitude))**2

        self.a = self.a + delta_a(a=self.a, c=c, magnitude=magnitude)
        self.theta += d_theta
        self.get_coords()
        if self.check_max():
            return self.coord
        return None  # Explicity set to None if past the end of path

    def sample_path(self, d_theta=np.pi/4, steps=10):
        theta_list = np.linspace(self.theta, self.theta+d_theta, steps)
        partial_conversion = partial(self.convert_latlong, a=self.a)
        return list(map(partial_conversion, theta_list))

    def convert_latlong(self, theta, a):
        r = np.exp(a*theta) * self.max_radius
        self.r_ = r
        lat = r * np.sin(theta) + self.center_coord[0]
        lon = r * np.cos(theta) + self.center_coord[1]
        return(lat,lon)
    
    def get_coords(self):
        self.coord = self.convert_latlong(theta=self.theta, a=self.a)
        return self.coord

    def check_max(self):
        if self.r_ > self.max_radius:
            return False
        return True