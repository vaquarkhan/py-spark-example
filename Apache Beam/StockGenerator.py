import numpy as np
import logging


logging.basicConfig(
    level = logging.INFO,
    format = '%(message)s',
    )
log = logging.getLogger(__name__)

class StockGenerator:
    '''Generates random-walk stock value from a provided mu, sigma,
       and starting stock value.
    '''

    def __init__(self, mu, sigma, starting_price = 100):
        self.mu = mu
        self.sigma = sigma
        self.stock_value = starting_price
        self.dist = np.random.normal(mu, sigma, 1000)

    def __next__(self):
        random_return = np.random.choice(self.dist, 1)
        self.stock_value = self.stock_value * random_return[0]
        log.info('New stock value: %s', self.stock_value)
        return self.stock_value
