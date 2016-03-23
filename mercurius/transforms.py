import datetime

def trim(line, cols=None):
    return list(col.strip() if type(col) is str else col for col in line)

def null_equal(line, cols, value):
    for col in cols:
        if line[col] == value:
            line[col] = None
    return line

def null_if(line, cols, fn):
    for col in cols:
        if fn(line[col]):
            line[col] = None
    return line

def combine(line, start, count, transform=None):
    line[start:start+count] = [line[start:start+count]]
    if transform is not None: line[start] = transform(line[start])
    return line

def parse_datetime(format):
    return lambda x: datetime.datetime.strptime(' '.join(x), format)

def join(sep, filter=None):
    return lambda x: sep.join(i for i in x if filter is None or filter(i)).strip()