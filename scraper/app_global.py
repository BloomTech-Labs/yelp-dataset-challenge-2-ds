class G():
    """g, Global Class
            Used to store repeatedly accessed information in the scraper module
    """
    def __init__(self, client=None):
        self.client=client

g = G()