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


def model_values(cls, values, excludes_pk=False):
    if isinstance(values, (dict, OrderedDict)):
        if excludes_pk:
            pks = {c.name for c in cls.columns if c.pk}
            values = {k:v for k,v in values.items() if k not in pks}
        return values
    elif isinstance(values, cls):
        return OrderedDict([(cv[0].name, cv[1]) for cv in values if not excludes_pk or not cv[0].pk])
    else:
        raise TypeError(f"Values to insert or update must be a dictionary or model.")