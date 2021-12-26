"""
Base module of pyracmon exporting commonly used objects.  Use `*` simply to import them.

>>> from pyracmon import *
"""
import sys
import types
from typing import *
from pyracmon.config import pyracmon, default_config
from pyracmon.connection import connect, Connection
from pyracmon.context import ConnectionContext
from pyracmon.graph.serialize import NodeSerializer
from pyracmon.mixin import CRUDMixin
from pyracmon.select import read_row
from pyracmon.model import define_model, Table, Column, Model
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.query import Q, Expression, Conditional, escape_like, where, order_by, ranged_by
from pyracmon.graph import new_graph, S
from pyracmon.graph.graph import Graph, GraphView, NodeContainer, ContainerView, Node, NodeView, NodeChildrenView
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import GraphTemplate
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
    "Expression",
    "Conditional",
    "where",
    "escape_like",
    "order_by",
    "ranged_by",
    "new_graph",
    "S",
    "Graph",
    "GraphView",
    "NodeContainer",
    "ContainerView",
    "Node",
    "NodeView",
    "NodeChildrenView",
    "TypedDict",
    "document_type",
    "Typeable",
    "walk_schema",
    "GraphSchema",
    "Model",
    "declare_models",
    "graph_template",
    "graph_dict",
    "graph_schema",
]


def declare_models(
    dialect: types.ModuleType,
    db: Connection,
    module: Union[types.ModuleType, str] = __name__,
    mixins: List[type] = [],
    excludes: List[str] = None,
    includes: List[str] = None,
) -> List[Type[Model]]:
    """
    Declare model types read from database into the specified module.

    Args:
        dialect: A module exporting `read_schema` function and `mixins` classes.
            `pyracmon.dialect.postgresql` and `pyracmon.dialect.mysql` are available.
        db: Connection already connected to database.
        module: A module or module name where the declarations are located.
        mixins: Additional mixin classes for declaring model types.
        excludes: Excluding table names.
        includes: Including table names. When this argument is omitted, all tables except for specified in `excludes` are declared.
    Returns:
        Declared model types.
    """
    tables = dialect.read_schema(db, excludes, includes)
    models = []
    for t in tables:
        m = define_model(t, mixins + dialect.mixins + [CRUDMixin, GraphEntityMixin, Model])
        if isinstance(module, types.ModuleType):
            module.__dict__[t.name] = m
        else:
            sys.modules[module].__dict__[t.name] = m
        models.append(m)
    return models


def graph_template(*bases: GraphTemplate, **definitions: type) -> GraphTemplate:
    """
    Create a graph template on the default `GraphSpec` predefined to handle model object in appropriate ways.

    See `pyracmon.graph.GraphSpec.new_template` for the detail of definitions.

    Args:
        bases: Base templates whose properties and relations are merged into new template.
        definitions: Definitions of template properties.
    Returns:
        Graph template.
    """
    return default_config().graph_spec.new_template(*bases, **definitions)


def graph_dict(graph: GraphView, **settings: NodeSerializer) -> Dict[str, Any]:
    """
    Serialize a graph into a `dict` under the default specification.

    See `pyracmon.graph.GraphSpec.to_dict` for the detail of serialization settings.

    Args:
        graph: A view of the graph.
        settings: Serialization settings.
    Returns:
        Serialization result.
    """
    return default_config().graph_spec.to_dict(graph, **settings)


def graph_schema(template: GraphTemplate, **settings: NodeSerializer) -> GraphSchema:
    """
    Creates `GraphSchema` under the default specifications.

    `GraphSchema` represents the structure of `dict` serialized a graph of the template with given serialization settings.
    Use this, for example, to document REST API which responds serialized graph in JSON format. 

    See `pyracmon.graph.GraphSpec.to_schema` for the detail of serialization settings.

    Args:
        template: A template of serializing graph.
        settings: Serialization settings.
    Returns:
        Schema of serialization result.
    """
    return default_config().graph_spec.to_schema(template, **settings)