"""
Base module of pyracmon, exporting commonly used objects. Import them all with `*`.

>>> from pyracmon import *
"""
import sys
import types
from typing import Any, TypeVar, TYPE_CHECKING
from pyracmon.config import default_config
from pyracmon.connection import connect, aconnect, Connection, AsyncConnection
from pyracmon.context import ConnectionContext, AsyncConnectionContext
from pyracmon.dbapi import cursor, acursor
from pyracmon.graph.serialize import NodeSerializer
from pyracmon.mixin import CRUDMixin
from pyracmon.mixin_async import AsyncCRUDMixin
from pyracmon.select import read_row
from pyracmon.model import define_model, Table, Column
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.query import Q, Expression, Conditional, escape_like, where
from pyracmon.query_graph import append_rows, append_rows_async
from pyracmon.clause import order_by, ranged_by, holders, values
from pyracmon.stub import output_stub
from pyracmon.graph import new_graph, S
from pyracmon.graph.graph import Graph, GraphView, NodeContainer, ContainerView, Node, NodeView
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.schema import document_type, Typeable, GraphSchema
from pyracmon.graph.serialize import NodeContext
from pyracmon.graph.typing import walk_schema
from pyracmon.graph.typed import TNode, TGraph, TEdge, TypedGraph, new_typed_graph, dump_typed_graph
from pyracmon.testing import TestingMixin, AsyncTestingMixin


if TYPE_CHECKING:
    from pyracmon.model import Model as _Model
    class Model(_Model):
        pass
else:
    from pyracmon.model import Model


__all__ = [
    "connect",
    "aconnect",
    "Connection",
    "AsyncConnection",
    "ConnectionContext",
    "AsyncConnectionContext",
    "cursor",
    "acursor",
    "CRUDMixin",
    "AsyncCRUDMixin",
    "read_row",
    "define_model",
    "Table",
    "Column",
    "Q",
    "Expression",
    "Conditional",
    "where",
    "append_rows",
    "append_rows_async",
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
    "declare_models_async",
    "graph_template",
    "graph_dict",
    "graph_schema",
    "TNode",
    "TGraph",
    "TEdge",
    "TypedGraph",
    "new_typed_graph",
    "dump_typed_graph",
]


M = TypeVar('M', bound=Model)


def declare_models(
    dialect: types.ModuleType,
    db: Connection,
    module: types.ModuleType | str = __name__,
    mixins: list[type] = [],
    excludes: list[str] | None = None,
    includes: list[str] | None = None,
    *,
    testing: bool = False,
    model_type: type[M] = Model,
    write_stub: bool = False,
) -> list[type[M]]:
    """
    Declare model types read from the database into the specified module.

    Args:
        dialect: A module exporting a `read_schema` function and `mixins` classes.
            `pyracmon.dialect.postgresql` and `pyracmon.dialect.mysql` are available.
        db: A `Connection` already connected to the database.
        module: A module, or its name, where the declared model types are defined.
        mixins: Additional mixin classes for declaring model types.
        excludes: Table names to exclude.
        includes: Table names to include. When this argument is omitted, all tables except those specified in `excludes` are declared.
        testing: If `True`, declared models additionally inherit `TestingMixin`.
        model_type: Base model type that declared models inherit.
        write_stub: If `True`, writes stub files for the declared models via `output_stub`.
    Returns:
        Declared model types.
    """
    tables = dialect.read_schema(db, excludes, includes)
    return _define_models(tables, False, dialect, module, mixins, testing, model_type, write_stub)


async def declare_models_async(
    dialect: types.ModuleType,
    db: AsyncConnection,
    module: types.ModuleType | str = __name__,
    mixins: list[type] = [],
    excludes: list[str] | None = None,
    includes: list[str] | None = None,
    *,
    testing: bool = False,
    model_type: type[M] = Model,
    write_stub: bool = False,
) -> list[type[M]]:
    """
    Declare model types read from the database into the specified module.

    Args:
        dialect: A module exporting a `read_schema` function and `mixins` classes.
            `pyracmon.dialect.postgresql` and `pyracmon.dialect.mysql` are available.
        db: A `Connection` already connected to the database.
        module: A module, or its name, where the declared model types are defined.
        mixins: Additional mixin classes for declaring model types.
        excludes: Table names to exclude.
        includes: Table names to include. When this argument is omitted, all tables except those specified in `excludes` are declared.
        testing: If `True`, declared models additionally inherit `AsyncTestingMixin`.
        model_type: Base model type that declared models inherit.
        write_stub: If `True`, writes stub files for the declared models via `output_stub`.
    Returns:
        Declared model types.
    """
    tables = await dialect.read_schema_async(db, excludes, includes)
    return _define_models(tables, True, dialect, module, mixins, testing, model_type, write_stub)


def _define_models(
    tables: list[Table],
    is_async: bool,
    dialect: types.ModuleType,
    module: types.ModuleType | str,
    mixins: list[type],
    testing: bool,
    model_type: type[M],
    write_stub: bool,
):
    models = []
    mod = module if isinstance(module, types.ModuleType) else sys.modules[module]
    crud_mixin = AsyncCRUDMixin if is_async else CRUDMixin
    base_mixins = [crud_mixin, GraphEntityMixin, model_type]
    if testing:
        base_mixins[0:0] = [AsyncTestingMixin] if is_async else [TestingMixin]
    for t in tables:
        dialect_mixins = dialect.async_mixins if is_async else dialect.mixins
        m = define_model(t, mixins + dialect_mixins + base_mixins)
        mod.__dict__[t.name] = m
        models.append(m)
    if write_stub:
        output_stub(None, mod, models, dialect, mixins, testing=testing, is_async=is_async)
    return models


def graph_template(*bases: GraphTemplate, **definitions: type) -> GraphTemplate:
    """
    Create a graph template on the default `GraphSpec`, which handles model objects appropriately.

    See `pyracmon.graph.GraphSpec.new_template` for details on definitions.

    Args:
        bases: Base templates whose properties and relations are merged into the new template.
        definitions: Definitions of template properties.
    Returns:
        Graph template.
    """
    return default_config().graph_spec.new_template(False, *bases, **definitions)


def graph_dict(graph: GraphView, **settings: NodeSerializer) -> dict[str, Any]:
    """
    Serialize a graph into a `dict` under the default `GraphSpec`.

    See `pyracmon.graph.GraphSpec.to_dict` for details on serialization settings.

    Args:
        graph: A view of the graph.
        settings: Serialization settings where each key denotes a node name.
    Returns:
        Serialization result.
    """
    return default_config().graph_spec.to_dict(graph, {}, **settings)


def graph_schema(template: GraphTemplate, **settings: NodeSerializer) -> GraphSchema:
    """
    Creates a `GraphSchema` under the default `GraphSpec`.

    See `pyracmon.graph.GraphSpec.to_schema` for details on serialization settings.

    Args:
        template: The template of the graph to serialize.
        settings: Serialization settings where each key denotes a node name.
    Returns:
        Schema of serialization result.
    """
    return default_config().graph_spec.to_schema(template, **settings)