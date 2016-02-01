import os
import csv
import glob
import datetime
from . import base

class CSVSource(base.NullSource):
    def __init__(self, mask, root='.', sort='time', header=0, encoding='utf-8'):
        super().__init__()
        self.root = root
        self.mask = mask if type(mask) is not str else [mask]
        self.sort = sort
        self.header = header
        self.enc = encoding
        self.file = None

    def list(self, since=None, complete=False):
        if complete: since = None

        gen = ({'name': os.path.abspath(file), 'time': datetime.datetime.fromtimestamp(os.path.getmtime(file), tz=datetime.timezone.utc).astimezone(tz=None)}
            for mask in self.mask for file in glob.glob(os.path.join(self.root, mask)) if not file.endswith('.error'))

        if since is not None:
            gen = filter(lambda x: x['time'] > since, gen)

        if self.sort is not None:
            gen = sorted(gen, key=lambda x: x[self.sort])

        return [(x['name'], x['time'], None) for x in gen]

    def extract(self, entry):
        if self.file is None:
            self.file = open(entry, 'r', encoding=self.enc)

        reader = csv.reader([line.replace('\00', '') for line in self.file])
        try: _ = list(zip(range(self.header), reader))
        except StopIteration: return

        for line in reader:
            yield line

    def fail(self, entry, exc=None):
        self.file.close()
        self.file = None
        os.replace(entry, entry + '.error')

    def succeed(self, entry):
        self.file.close()
        self.file = None
        if os.path.isfile(entry + '.error'):
            os.remove(entry + '.error')
