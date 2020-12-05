class Marker:
    """
    This class provides the abstration mechanism for marker creation used to embed parameters in a query.

    Each inheriting type corresponds to `paramstyle` defined in DB-API 2.0.

    The instance of `Marker` is callable object designed to return place holder string available in query.
    """
    @classmethod
    def of(cls, paramstyle):
        """
        Select marker type from `paramstyle` and creates its instance.
        """
        if paramstyle == 'qmark':
            return QMarker()
        elif paramstyle == 'numeric':
            return NumericMarker()
        elif paramstyle == 'named':
            return NamedMarker()
        elif paramstyle == 'format':
            return FormatMarker()
        elif paramstyle == 'pyformat':
            return PyformatMarker()
        else:
            raise ValueError(f"Unknown parameter style: {paramstyle}")

    def __call__(self, key=None):
        """
        Renders a place holder string for the key.

        Parameters
        ----------
        key: int | str
            A key specifying the parameter which will be given on SQL execution.

        Returns
        -------
        str
            A place holder string.
        """
        if key is None:
            return self.default()
        elif isinstance(key, int) and key > 0:
            return self.indexed(key)
        elif isinstance(key, str) and key != "":
            return self.keyed(key)
        else:
            raise ValueError("Argument of marker invocation must be a positive int or str.")

    def default(self):
        raise NotImplementedError()

    def indexed(self, index):
        raise NotImplementedError()

    def keyed(self, key):
        raise NotImplementedError()

    def reset(self):
        """
        Resets the internal state.
        """
        pass

    def params(self, *args, **kwargs):
        """
        Creates a holder containing parameters in the form available for the marker type.

        Parameters
        ----------
        args: [object]
            Parameters for indexed place holder.
        kwargs: {str: object}
            Parameters for keyed place holder.

        Returns
        -------
        [object] | {str:object}
            List or dictionary which has parameters at their valid index or key.
        """
        raise NotImplementedError()


class ListMarker(Marker):
    def __init__(self, start=0):
        self.index = start
        self.param_keys = []

    def render(self):
        raise NotImplementedError()

    def default(self):
        self.param_keys.append(self.index)
        self.index += 1
        return self.render()

    def indexed(self, index):
        self.param_keys.append(index-1)
        return self.render()

    def keyed(self, key):
        self.param_keys.append(key)
        return self.render()

    def reset(self):
        self.index = 0
        self.param_keys = []

    def params(self, *args, **kwargs):
        def get(k):
            if isinstance(k, int):
                return args[k]
            else:
                return kwargs[k]

        return [get(k) for k in self.param_keys]


class DictMarker(Marker):
    def __init__(self, start=0):
        self.index = start
        self.param_keys = {}

    def render(self, key):
        raise NotImplementedError()

    def new_key(self, index):
        return f"param{index}"

    def default(self):
        key = self.param_keys.setdefault(self.index, self.new_key(self.index+1))
        self.index += 1
        return self.render(key)

    def indexed(self, index):
        key = self.param_keys.setdefault(index-1, self.new_key(index))
        return self.render(key)

    def keyed(self, key):
        self.param_keys[key] = key
        return self.render(key)

    def reset(self):
        self.index = 0
        self.param_keys = {}

    def params(self, *args, **kwargs):
        def get(k, v):
            if isinstance(k, int):
                return (v, args[k])
            else:
                return (v, kwargs[k])

        return dict([get(k, v) for k, v in self.param_keys.items()])


class QMarker(ListMarker):
    """
    This marker renders `?` for any parameter.
    """
    def render(self):
        return '?'


class NumericMarker(ListMarker):
    """
    This marker renders `:x` for the parameter at index `x` (starting from 1).

    Automatic numbering is used when invoked with no `key`, otherwise specified position is selected.
    The index obtained by next automatic numbering is an incremented value of the last obtained index.
    """
    def render(self):
        return f":{len(self.param_keys)}"


class NamedMarker(DictMarker):
    """
    This marker renders `:key` for the parameter bound to `key`.

    Automatic key selection is used when invoked with no `key`, otherwise specified key is selected.
    Each automatic key is in the form of `keyn` where `n` is specified index or its order in generated keys.
    """
    def render(self, key):
        return f":{key}"


class FormatMarker(ListMarker):
    """
    This marker renders `%s` for any parameter.
    """
    def render(self):
        return '%s'


class PyformatMarker(DictMarker):
    """
    This marker renders `%(key)s` for the parameter bound to `key`.

    This works similarly to `NamedMarker`.
    """
    def render(self, key):
        return f"%({key})s"
