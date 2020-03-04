import sys
import types
from pyracmon.connection import connect, Connection
from pyracmon.mixin import CRUDMixin, read_row
from pyracmon.model import define_model
from pyracmon.graph import new_graph, GraphSpec, S
from pyracmon.query import Q


__all__ = [
    "connect",
    "Connection",
    "declare_models",
    "read_row",
    "graph_template",
    "graph_dict",
    "add_identifier",
    "add_serializer",
    "new_graph",
    "S",
    "Q",
]


def declare_models(dialect, db, module = __name__, mixins = [], excludes = [], includes = []):
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


class GraphEntityMixin:
    @classmethod
    def identify(cls, model):
        pks = [c.name for c in cls.columns if c.pk]
        if len(pks) > 0 and all([hasattr(model, n) and getattr(model, n) is not None for n in pks]):
            return tuple(map(lambda n: getattr(model, n), pks))
        else:
            return None

    @classmethod
    def is_null(cls, model):
        return all([getattr(model, c.name, None) is None for c in cls.columns])

def _identify(model):
    return type(model).identify(model)

def _filter(model):
    return model and not type(model).is_null(model)

def _serialize(s, model):
    return dict([(c.name, v) for c, v in model])


globalSpec = GraphSpec(
    identifiers=[
        (GraphEntityMixin, _identify),
    ],
    entity_filters=[
        (GraphEntityMixin, _filter),
    ],
    serializers=[
        (GraphEntityMixin, _serialize),
    ]
)

def graph_template(*bases, **definitions):
    """
    Create a graph template on the default specification predefined to handle model object in appropriate ways.

    Parameters
    ----------
        bases: [GraphTemplate]
            Base templates whose properties and relations are merged into new template.
    definitions: {str: (type, T -> ID, T -> bool) | type | None}
        Definitions of template properties. See `GraphSpec.new_template` for the detail.

    Returns
    -------
    GraphTemplate
        Created graph template.
    """
    return globalSpec.new_template(*bases, **definitions)


def graph_dict(graph, **serializers):
    """
    Generates a dictionary representing structured values of a graph on the default specification predefined to handle model object in appropriate ways.

    Parameters
    ----------
    graph: Graph.View
        A view of the graph.
    serializers: {str: NodeSerializer | (str | str -> str, [T] -> T | int, T -> U)}
        Mapping from property name to `NodeSerializer` s or their equivalents.

    Returns
    -------
    {str: object}
        A dictionary representing the graph.
    """
    return globalSpec.to_dict(graph, **serializers)


def add_identifier(t, identifier):
    """
    Register a function which extracts identifying key value from the entity to the default specification.

    Be sure to call this function before every invocation of `graph_template()`.

    Parameters
    ----------
    c: type
        Super type of the entity to apply the function.
    identifier: T -> ID
        A function which extracts identifying key value from the entity.
    """
    globalSpec.add_identifier(t, identifier)


def add_entity_filter(t, entity_filter):
    """
    Register a function which determines whether to append the entity into the graph to the default specification.

    Be sure to call this function before every invocation of `graph_template()`.

    Parameters
    ----------
    c: type
        Super type of the entity to apply the function.
    entity_filter: T -> bool
        A function which determines whether to append the entity into the graph.
    """
    globalSpec.add_entity_filter(t, entity_filter)


def add_serializer(t, serializer):
    """
    Register a function which converts the entity into a serializable value to the default specification.

    Parameters
    ----------
    c: type
        Super type of the entity to apply the function.
    serializer: T -> U
        A function which converts the entity into a serializable value.
    """
    globalSpec.add_serializer(t, serializer)