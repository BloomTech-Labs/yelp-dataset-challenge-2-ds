"""
Path Classes

    Expose consistent movement and path sampling, jumping, analysis for 
    various classes.
"""

import numpy as np 


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
        pass

    def convert_latlong(self):
        NotImplemented


class SpiralPath(Path):
    """
    Logistic Sprial Path
    r = 
    """
    def __init__(self, step_size=np.pi/12, a=0.05):
        self.theta = 0
        self.a = a

        super().__init__(step_size=step_size, start_coord=start_coord)

    def move(self, theta, d_theta=None, c=0.025, step=1):
        if d_theta is None:
            d_theta = self.step_size
        # Calculate expected value for d_theta and adjust curvature
        def delta_a(a, expected_value, c):
            return c * a * (50-expected_value)/expected_value
        
        # TODO: Calling the prediction from the path module directly doesn't make much sense.
        #   Move can take an input (expected value) and any other parameters it needs,
        #   but, those parameters should be set at the scraper level.
        expected_value = predict_capture(
                            self.sample_path(d_theta=d_theta)
                            )
        a = self.a
        self.a = a + delta_a(a, c, expected_value)

    def sample_path(self):
        #TODO
        pass

    def convert_latlong(self, theta, a):
        r = np.exp(-a*theta)
        lat = r * np.sin(theta)
        lon = r * np.cos(theta)
        return(lat,lon)