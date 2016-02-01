
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
