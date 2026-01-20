import time

class RateLimiter:
    def __init__(self, rpm: int):
        self.interval = 60.0 / rpm
        self.last_call = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_call = time.time()
