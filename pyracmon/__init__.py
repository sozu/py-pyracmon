import sys
from pyracmon.mixin import CRUDMixin
from pyracmon.model import define_model

def declare_models(dialect, db, module = __name__, mixins = [], excludes = [], includes = []):
    """
    Declare model types read from database in the specified module.

    Parameters
    ----------
    dialect: module
        A module exporting `read_schema` function and `mixins` classes.
    db: pyracmon.connection.Connection
        Wrapper of DB-API 2.0 Connection.
    module: str
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
        sys.modules[module].__dict__[t.name] = define_model(t, mixins + dialect.mixins + [CRUDMixin])