from functools import reduce
from collections import OrderedDict

def split_dict(d):
    def add(acc, kv):
        acc[0].append(kv[0])
        acc[1].append(kv[1])
        return acc
    return reduce(add, d.items(), ([], []))


def index_qualifier(qualifier, ordered_names):
    def index(k):
        if isinstance(k, int):
            return k
        else:
            return ordered_names.index(k)

    return dict([(index(k), q) for k, q in qualifier.items()])


def model_values(cls, values):
    if isinstance(values, (dict, OrderedDict)):
        return values
    elif isinstance(values, cls):
        return OrderedDict([(cv[0].name, cv[1]) for cv in values])
    else:
        raise TypeError(f"Values to insert or update must be a dictionary or model.")