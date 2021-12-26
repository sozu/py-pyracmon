from string import digits, Template
from typing import *
from .marker import Marker


class Sql:
    """
    Provides functionalities to render SQL string from the template containing placeholder markers.

    SQL rendering is conform to the way of `string.Template` which replaces `$` prefixed variables with parameters.
    """
    class Substitute:
        def __init__(self, marker: Marker):
            self.marker = marker

        def __getitem__(self, key):
            if key == "_":
                return self.marker()
            elif key[0] == "_" and all([c in digits for c in key[1:]]):
                return self.marker(int(key[1:]))
            else:
                return self.marker(key)

    def __init__(self, marker: Marker, template: str):
        #: Marker used in the template
        self.marker = marker
        #: SQL template.
        self.template = template

    def render(self, *args: Any, **kwargs: Any) -> Tuple[str, List[Any]]:
        """
        Renders SQL and converts parameters into the form available for current database driver.

        Arguments type should be consistent to marker paramstyle, otherwise exception is raised.

        Args:
            args: Positional parameters.
            kwargs: Keyed parameters.
        Returns:
            SQL and parameters available in it.
        """
        self.marker.reset()

        sub = Sql.Substitute(self.marker)

        return Template(self.template).substitute(sub), self.marker.params(*args, **kwargs)


