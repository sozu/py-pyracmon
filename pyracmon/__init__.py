import sys
import types
from pyracmon.config import pyracmon, default_config
from pyracmon.connection import connect, Connection
from pyracmon.context import ConnectionContext
from pyracmon.mixin import CRUDMixin
from pyracmon.select import read_row
from pyracmon.model import define_model, Table, Column
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.query import Q, Conditional, where
from pyracmon.graph import new_graph, GraphSpec, S
from pyracmon.graph.schema import TypedDict, document_type, Typeable, walk_schema, GraphSchema


__all__ = [
    "pyracmon",
    "connect",
    "Connection",
    "ConnectionContext",
    "CRUDMixin",
    "read_row",
    "define_model",
    "Table",
    "Column",
    "Q",
    "Conditional",
    "where",
    "new_graph",
    "S",
    "TypedDict",
    "document_type",
    "Typeable",
    "walk_schema",
    "GraphSchema",
    "declare_models",
    "graph_template",
    "graph_dict",
    "graph_schema",
]


def declare_models(dialect, db, module=__name__, mixins=[], excludes=None, includes=None):
    """
    Declare model types read from database in the specified module.

    Parameters
    ----------
    dialect: module
        A module exporting `read_schema` function and `mixins` classes.
    db: pyracmon.connection.Connection
        Wrapper of DB-API 2.0 Connection.
    module: str | module
        A module name where the declarations are located.
    mixins: [type]
        Additional mixin classes for declaring model types.
    excludes: [str]
        Excluding table names.
    includes: [str]
        Including table names. All tables excluding `excludes` are declared as models if this argument is omitted.
    """
    tables = dialect.read_schema(db, excludes, includes)
    for t in tables:
        if isinstance(module, types.ModuleType):
            module.__dict__[t.name] = define_model(t, mixins + dialect.mixins + [CRUDMixin, GraphEntityMixin])
        else:
            sys.modules[module].__dict__[t.name] = define_model(t, mixins + dialect.mixins + [CRUDMixin, GraphEntityMixin])


def graph_template(*bases, **definitions):
    """
    Create a graph template on the default specification predefined to handle model object in appropriate ways.

    Parameters
    ----------
    bases: [GraphTemplate]
        Base templates whose properties and relations are merged into new template.
    template: {str: (type | Tuple[type, T -> ID, T -> bool])}
        Definitions of template properties.

    Returns
    -------
    GraphTemplate
        Created graph template.
    """
    return default_config().graph_spec.new_template(*bases, **definitions)


def graph_dict(graph, **settings):
    """
    Generates a dictionary representing structured values of a graph under the default specification.

    Parameters
    ----------
    graph: GraphView
        A view of the graph.
    settings: {str: NodeSerializer}
        Mapping from property name to `NodeSerializer` s.

    Returns
    -------
    {str: object}
        A dictionary representing the graph.
    """
    return default_config().graph_spec.to_dict(graph, **settings)


def graph_schema(template, **settings):
    """
    Creates `GraphSchema` under the default specifications.

    Parameters
    ----------
    template: GraphTemplate
        A template of serializing graph.
    settings: {str: NodeSerializer}
        Mapping from property name to `NodeSerializer` s.

    Returns
    -------
    GraphSchema
        An object having schema information of serialized dictionary.
    """
    return default_config().graph_spec.to_schema(template, **settings)