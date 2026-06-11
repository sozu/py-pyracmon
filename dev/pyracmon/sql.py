"""
This module provides a type for generating queries from a template string containing unified markers.
"""
from string import digits, Template
from typing import Any
from .marker import Marker


class Sql:
    """
    Provides functionality to render an SQL string from a template containing placeholder markers.

    SQL rendering follows the convention of `string.Template`, which replaces `$`-prefixed variables with parameters.
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

    def __init__(self, marker: Marker, template: str) -> None:
        #: Marker used in the template.
        self.marker = marker
        #: SQL template.
        self.template = template

    def render(self, *args: Any, **kwargs: Any) -> tuple[str, list[Any] | dict[str, Any]]:
        """
        Renders SQL and converts parameters into the form expected by the current database driver.

        Argument types should be consistent with the marker's paramstyle, otherwise an exception is raised.

        Args:
            args: Positional parameters.
            kwargs: Keyed parameters.
        Returns:
            The rendered SQL string and the parameters referenced in it.
        """
        self.marker.reset()

        sub = Sql.Substitute(self.marker)

        return Template(self.template).substitute(sub), self.marker.params(*args, **kwargs) # type: ignore