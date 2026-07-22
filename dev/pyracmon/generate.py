import builtins
from collections.abc import Iterator, Sequence
from keyword import iskeyword
import inspect
from typing import Any, ClassVar, TextIO, get_origin, get_args, TYPE_CHECKING
from .model import Model, Table, Column, Relations


class ModelBase:
    if TYPE_CHECKING:
        name: ClassVar[str]
        table: ClassVar[Table]
        columns: ClassVar[list[Column]]

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        cls = type(self)
        return f"{cls.name}({', '.join([f'{c.name}={repr(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

    def __str__(self):
        cls = type(self)
        return f"{cls.name}({', '.join([f'{c.name}={str(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

    def __iter__(self) -> Iterator[tuple[Column, Any]]:
        cls = type(self)
        return map(lambda c: (c, getattr(self, c.name)), filter(lambda c: hasattr(self, c.name), cls.columns))

    def __setattr__(self, key, value):
        cls = type(self)
        if key not in {c.name for c in cls.columns}:
            raise TypeError(f"{key} is not a column of {cls.name}")
        object.__setattr__(self, key, value)

    def __getitem__(self, key: str | Column):
        cls = type(self)
        if isinstance(key, Column):
            if key not in cls.columns:
                raise ValueError(f"{key.name} is not a column of {cls.name}")
            key = key.name
        return getattr(self, key)

    def __contains__(self, key: str | Column):
        if isinstance(key, Column):
            if key not in type(self).columns:
                return False
            key = key.name
        return hasattr(self, key)

    def __eq__(self, other):
        cls = type(self)
        if cls != type(other):
            return False
        for k in {c.name for c in cls.columns}:
            if hasattr(self, k) ^ hasattr(other, k):
                return False
            if getattr(self, k, None) != getattr(other, k, None):
                return False
        return True


def _get_coltype(t: type) -> tuple[type, str]:
    org = get_origin(t)
    org = org or t

    if org in (object, dict):
        return org, org.__name__
    elif org is list:
        elems = [_get_coltype(et) for et in get_args(t)]
        if elems:
            return elems[0][0], f"list[{elems[0][1]}]"
        else:
            return object, "list"
    else:
        return t, t.__name__


def _render_relations(rel: Relations) -> str:
    constraints = []

    for fk in rel.constraints:
        t = fk.table.name if isinstance(fk.table, Table) else fk.table
        c = fk.column.name if isinstance(fk.column, Column) else fk.column
        constraints.append(f"ForeignKey('{t}', '{c}')")

    return f"Relations([{', '.join(constraints)}])"


def _render_column(column: Column) -> str:
    ptype = _get_coltype(column.ptype)
    if ptype[0] is Any:
        ptype = [object, "object"]

    return f"""
        Column(
            name="{column.name}",
            ptype={ptype[1]},
            type_info={repr(column.type_info)},
            pk={repr(column.pk)},
            fk={'None' if column.fk is None else _render_relations(column.fk)},
            incremental={repr(column.incremental)},
            nullable={repr(column.nullable)},
            comment="{column.comment.replace('"', '\\"')}",
        )"""


def _render_model(
    model: type[Model],
    additional_imports: dict[str, set[str]],
) -> str:
    super_types: list[str] = []

    for mx in model.__bases__[0].__bases__[1:]:
        if isinstance(mx, type):
            if mod := inspect.getmodule(mx):
                additional_imports.setdefault(mod.__name__, set()).add(mx.__name__)
                super_types.append(mx.__name__)

    code = f"""
{model.table.name}_table = Table(
    name="{model.table.name}",
    columns=[{','.join([_render_column(c) for c in model.table.columns])}],
    comment="{model.table.comment.replace('"', '\\"')}",
)
"""

    code += f"""
class {model.name}_meta(Meta, type):
    name = "{model.name}"
    table = {model.table.name}_table
    columns = ModelColumns({model.table.name}_table.columns)
"""

    code += f"""
class {model.name}({', '.join([ModelBase.__name__] + super_types)}, metaclass={model.name}_meta):
    if TYPE_CHECKING:
"""

    for c in model.columns:
        ct, tstr = _get_coltype(c.ptype)
        if mod := inspect.getmodule(ct):
            if mod != builtins:
                additional_imports.setdefault(mod.__name__, set()).add(ct.__name__)
        if c.nullable:
            ct = f"{tstr} | None"
        if c.name.isidentifier() and not iskeyword(c.name):
            code += f"""\
        {c.name}: {tstr}
"""
        else:
            code += f"""\
#        {c.name}: {tstr}
"""

    return code


def generate_code(
    models: Sequence[type[Model]],
) -> str:
    base_imports: dict[str, set[str]] = {
        "datetime": {"datetime", "date", "time", "timedelta"},
        "decimal": {"Decimal"},
        "enum": {"Enum"},
        "uuid": {"UUID"},
        "typing": {"Any", "TYPE_CHECKING"},
        "pyracmon": {"Model", "Table", "Column", "CRUDMixin", "AsyncCRUDMixin"},
        "pyracmon.generate": {"ModelBase"},
        "pyracmon.model": {"Meta", "ForeignKey", "Relations", "ModelColumns", "ModelColumns"},
        "pyracmon.model_graph": {"GraphEntityMixin"},
        "pyracmon.stub": {"ModelTransform"},
        "pyracmon.testing": {"TestingMixin", "AsyncTestingMixin"},
    }
    model_codes: list[str] = []

    for m in models:
        model_codes.append(_render_model(m, base_imports))

    code = ""

    for pac, mods in base_imports.items():
        code += f"""\
from {pac} import {', '.join(sorted(mods))}
"""

    code += "\n\n"

    for c in model_codes:
        code += f"{c}\n"

    return code


def main():
    import asyncio
    import argparse
    import importlib
    from pyracmon.connection import connect, aconnect
    from . import declare_models, declare_models_async

    parser = argparse.ArgumentParser(description="Output type stubs for model types.")
    parser.add_argument("driver", help="The database driver to connect to the database.")
    parser.add_argument("dsn", help="The DSN to connect to the database.")
    parser.add_argument("dialect", help="The database dialect to use.")
    parser.add_argument("--async", help="The name of the function or method to obtain an asynchronous connection.", dest="async_")

    args = parser.parse_args()

    driver = importlib.import_module(args.driver)

    dialect_module = f"pyracmon.dialect.{args.dialect}"
    dialect = importlib.import_module(dialect_module)

    if connector_name := args.async_:
        connector = driver
        for n in connector_name.split("."):
            connector = getattr(connector, n)
        async def run_async():
            db = await aconnect(connector, args.dsn, api=driver)
            return await declare_models_async(dialect, db, __name__, testing=True)
        models = asyncio.run(run_async())
    else:
        db = connect(driver, args.dsn)
        models = declare_models(dialect, db, __name__, testing=True)

    print(generate_code(models))


if __name__ == "__main__":
    main()