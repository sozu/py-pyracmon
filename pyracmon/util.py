class Configurable:
    """
    Interface for classes of configuration objects.
    """
    def clone(self):
        raise NotImplementedError()

    def replace(self, another):
        raise NotImplementedError()


def key_to_index(values, ordered_keys):
    """
    Generates a dictionary whose values are same as given dictionary but each key is an index of original key in the ordered key list.

    Parameters
    ----------
    values: {(str|int): object}
        A dictionary.
    ordered_keys: [str]
        Ordered key list. If some original keys are not contained in this list, `ValueError` raises.
    """
    def index(k):
        if isinstance(k, int):
            return k
        else:
            return ordered_keys.index(k)

    return {index(k):v for k, v in values.items()}