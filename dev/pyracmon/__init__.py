"""
Base module of pyracmon exporting commonly used objects.  Use `*` simply to import them.

>>> from pyracmon import *
"""
import sys
import types
from typing import Union, Optional, Any, TypeVar, TYPE_CHECKING
from pyracmon.config import default_config
from pyracmon.connection import connect, Connection
from pyracmon.context import ConnectionContext
from pyracmon.graph.serialize import NodeSerializer
from pyracmon.mixin import CRUDMixin
from pyracmon.select import read_row
from pyracmon.model import define_model, Table, Column
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.query import Q, Expression, Conditional, escape_like, where
from pyracmon.query_graph import append_rows
from pyracmon.clause import order_by, ranged_by, holders, values
from pyracmon.stub import output_stub
from pyracmon.graph import new_graph, S
from pyracmon.graph.graph import Graph, GraphView, NodeContainer, ContainerView, Node, NodeView
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.schema import document_type, Typeable, GraphSchema
from pyracmon.graph.serialize import NodeContext
from pyracmon.graph.typing import walk_schema
from pyracmon.testing import TestingMixin


if TYPE_CHECKING:
    from pyracmon.model import Model as _Model
    class Model(_Model):
        pass
else:
    from pyracmon.model import Model


__all__ = [
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
    "append_rows",
    "escape_like",
    "order_by",
    "ranged_by",
    "holders",
    "values",
    "new_graph",
    "S",
    "Graph",
    "GraphView",
    "NodeContainer",
    "ContainerView",
    "Node",
    "NodeView",
    "document_type",
    "Typeable",
    "walk_schema",
    "GraphSchema",
    "NodeContext",
    "Model",
    "declare_models",
    "graph_template",
    "graph_dict",
    "graph_schema",
]


M = TypeVar('M', bound=Model)


def declare_models(
    dialect: types.ModuleType,
    db: Connection,
    module: Union[types.ModuleType, str] = __name__,
    mixins: list[type] = [],
    excludes: Optional[list[str]] = None,
    includes: Optional[list[str]] = None,
    *,
    testing: bool = False,
    model_type: type[M] = Model,
    write_stub: bool = False,
) -> list[type[M]]:
    """
    Declare model types read from database into the specified module.

    Args:
        dialect: A module exporting `read_schema` function and `mixins` classes.
            `pyracmon.dialect.postgresql` and `pyracmon.dialect.mysql` are available.
        db: Connection already connected to database.
        module: A module or module name where the declarations will be located.
        mixins: Additional mixin classes for declaring model types.
        excludes: Excluding table names.
        includes: Including table names. When this argument is omitted, all tables except for specified in `excludes` are declared.
    Returns:
        Declared model types.
    """
    tables = dialect.read_schema(db, excludes, includes)
    models = []
    mod = module if isinstance(module, types.ModuleType) else sys.modules[module]
    base_mixins = [CRUDMixin, GraphEntityMixin, model_type]
    if testing:
        base_mixins[0:0] = [TestingMixin]
    for t in tables:
        m = define_model(t, mixins + dialect.mixins + base_mixins)
        mod.__dict__[t.name] = m
        models.append(m)
    if write_stub:
        output_stub(None, mod, models, dialect, mixins, testing=testing)
    return models


def graph_template(*bases: GraphTemplate, **definitions: type) -> GraphTemplate:
    """
    Create a graph template on the default `GraphSpec` which handles model object in appropriate ways.

    See `pyracmon.graph.GraphSpec.new_template` for the detail of definitions.

    Args:
        bases: Base templates whose properties and relations are merged into new template.
        definitions: Definitions of template properties.
    Returns:
        Graph template.
    """
    return default_config().graph_spec.new_template(*bases, **definitions)


def graph_dict(graph: GraphView, **settings: NodeSerializer) -> dict[str, Any]:
    """
    Serialize a graph into a `dict` under the default `GraphSpec` .

    See `pyracmon.graph.GraphSpec.to_dict` for the detail of serialization settings.

    Args:
        graph: A view of the graph.
        settings: Serialization settings where each key denotes a node name.
    Returns:
        Serialization result.
    """
    return default_config().graph_spec.to_dict(graph, {}, **settings)


def graph_schema(template: GraphTemplate, **settings: NodeSerializer) -> GraphSchema:
    """
    Creates `GraphSchema` under the default `GraphSpec` .

    See `pyracmon.graph.GraphSpec.to_schema` for the detail of serialization settings.

    Args:
        template: A template of serializing graph.
        settings: Serialization settings where each key denotes a node name.
    Returns:
        Schema of serialization result.
    """
    return default_config().graph_spec.to_schema(template, **settings)