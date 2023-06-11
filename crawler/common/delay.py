import random
import time


class Delay:
    def __init__(self, lb=1, ub=5, mu=0, sigma=0.5):
        super().__init__()
        self.lb = lb
        self.ub = ub
        self.mu = mu
        self.sigma = sigma

    def delay(self, second):
        random_second = random.randint(self.lb, self.ub) * random.uniform(1.0, 2.0) + random.gauss(self.mu, self.sigma)
        if random_second < 0:
            random_second = -random_second
        print(second + random_second)
        time.sleep(random_second)
        return
    
    def seconds(self, second):
        random_second = random.randint(self.lb, self.ub) * random.uniform(1.0, 2.0) + random.gauss(self.mu, self.sigma)
        print(second + random_second)
        return random_second

    
    def sleep(self, second):
        time.sleep(second)
        return