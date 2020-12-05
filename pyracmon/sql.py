from string import digits, Template
from .marker import Marker


class Sql:
    """
    Provides functionalities to render SQL string from the template containing place holders.

    SQL rendering is conform to the way of `string.Template` which replaces `$` prefixed variables with parameters.
    """
    class Substitute:
        def __init__(self, marker):
            self.marker = marker

        def __getitem__(self, key):
            if key == "_":
                return self.marker()
            elif key[0] == "_" and all([c in digits for c in key[1:]]):
                return self.marker(int(key[1:]))
            else:
                return self.marker(key)

    def __init__(self, marker, template):
        self.marker = marker
        self.template = template

    def render(self, *args, **kwargs):
        """
        Renders SQL and converts parameters into the form available for current database driver.

        Parameters
        ----------
        params: object
            Parameters. Raising an exception when the type of this object does not conform to the paramstyle.

        Returns
        -------
        str
            SQL string.
        object
            Values available as parameters of the SQL.
        """
        self.marker.reset()

        sub = Sql.Substitute(self.marker)

        return Template(self.template).substitute(sub), self.marker.params(*args, **kwargs)


