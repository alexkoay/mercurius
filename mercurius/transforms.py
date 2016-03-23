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

def combine(line, col, count, join=' ', suppress=False, datefmt=None):
    line[col:col+count] = [join.join(i for i in line[col:col+count] if i)]
    if datefmt is not None: line[col] = datetime.datetime.strptime(line[col], format)
    return line

def combine_datetime(line, col, format):
    line[col:col+2] = [datetime.datetime.strptime(' '.join(line[col:col+2]), format)]
    return line