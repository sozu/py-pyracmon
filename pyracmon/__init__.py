import sys
import types
from pyracmon.mixin import CRUDMixin, read_row
from pyracmon.model import define_model
from pyracmon.graph import GraphSpec


__all__ = ["declare_models", "read_row", "graph_template", "graph_dict"]


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
        if all([hasattr(model, n) for n in pks]):
            return tuple(map(lambda n: getattr(model, n), pks))
        else:
            return None

def _identify(model):
    return type(model).identify(model)

def _serialize(model):
    return dict([(c.name, v) for c, v in model])


globalSpec = GraphSpec(
    identifiers=[
        (GraphEntityMixin, _identify),
    ],
    serializers=[
        (GraphEntityMixin, _serialize),
    ]
)

def graph_template(**definitions):
    return globalSpec.new_template(**definitions)


def graph_dict(__graph__, **serializers):
    return globalSpec.to_dict(__graph__, **serializers)
