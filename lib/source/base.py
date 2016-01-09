
class NullSource:
    def list(self, since=-1, complete=False): return []
    def extract(self, entry): yield None
    def fail(self, entry, exc=None): pass
    def succeed(self, entry): pass

class ConstantSource:
    def __init__(self, data, n=1):
        self.data = data
        self.n = 1

    def list(self, since=-1, complete=False): return list(range(self.n))
    def extract(self, entry): yield self.data
