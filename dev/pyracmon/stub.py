"""
This module exports functions to output type stubs for model types.
"""
from keyword import iskeyword
import inspect
import os
from pathlib import Path
import types
from typing import get_origin, get_args, dataclass_transform
from pyracmon.model import Model
from pyracmon.mixin import CRUDMixin
from pyracmon.mixin_async import AsyncCRUDMixin
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.testing import TestingMixin, AsyncTestingMixin


default_imports = [
    ("typing", ["Any"]),
    ("pyracmon", ["Model", "CRUDMixin", "AsyncCRUDMixin"]),
    ("pyracmon.model_graph", ["GraphEntityMixin"]),
    ("pyracmon.stub", ["ModelTransform"]),
    ("pyracmon.testing", ["TestingMixin", "AsyncTestingMixin"]),
]


@dataclass_transform(kw_only_default=True)
class ModelTransform:
    pass


def render_models(
    models: list[type[Model]],
    dialect: types.ModuleType,
    mixins: list[type],
    testing: bool = False,
    is_async: bool = False,
) -> list[str]:
    """
    Generate the lines of a type stub file (.pyi).

    Args:
        models: A list of model types.
        dialect: The database dialect module.
        mixins: Mixin types used to declare the model types.
        testing: If `True`, the model classes additionally inherit `TestingMixin`.
        is_async: If `True`, the model classes are declared as asynchronous models.
    Returns:
        The lines of the generated type stub file.
    """
    lines = []

    super_types: list[str] = []
    additional_imports: dict[str, set[str]] = {}

    dialect_mixins = dialect.async_mixins if is_async else dialect.mixins
    for mx in mixins + dialect_mixins:
        if isinstance(mx, type):
            mod = inspect.getmodule(mx)
            if mod:
                additional_imports.setdefault(mod.__name__, set()).add(mx.__name__)
                super_types.append(mx.__name__)

    for m in models:
        for c in m.columns:
            mod = inspect.getmodule(c.ptype)
            if mod and mod.__name__ != 'builtins':
                additional_imports.setdefault(mod.__name__, set()).add(c.ptype.__name__)

    # import
    for mod, names in (default_imports + [(m,list(ns)) for m, ns in additional_imports.items()]):
        lines.append(f"from {mod} import {', '.join(names)}")

    base_mixins = [AsyncCRUDMixin if is_async else CRUDMixin, GraphEntityMixin, ModelTransform, Model]
    if testing:
        base_mixins[0:0] = [AsyncTestingMixin if is_async else TestingMixin]
    super_types.extend([m.__name__ for m in base_mixins])

    def coltype(t: type) -> str:
        org = get_origin(t)
        org = org or t

        if org in (object, dict):
            return "Any"
        elif org is list:
            elems = [coltype(et) for et in get_args(t)]
            if elems:
                return f"list[{', '.join(elems)}]"
            else:
                return "list"
        else:
            return t.__name__

    # model classes
    for m in models:
        lines.append("")
        lines.append(f"class {m.name}({', '.join(super_types)}):")
        for c in m.columns:
            ct = coltype(c.ptype)
            if c.nullable:
                ct = f"{ct} | None"
            if c.name.isidentifier() and not iskeyword(c.name):
                lines.append(f"    {c.name}: {ct} = ...")
            else:
                lines.append(f"#    {c.name}: {ct} = ...")

    return lines


def output_stub(
    stubdir: str | Path | None,
    module: types.ModuleType,
    models: list[type[Model]],
    dialect: types.ModuleType,
    mixins: list[type],
    testing: bool = False,
    is_async: bool = False,
):
    """
    Output a type stub file (.pyi) to the specified location.

    Args:
        stubdir: The directory to output to. If `None`, the stub file is written to the same directory as the module.
        module: The module where the model types are declared.
        models: The model types to generate stubs for.
        dialect: The database dialect module.
        mixins: The mixin types used to declare the model types.
        testing: If `True`, the model classes additionally inherit `TestingMixin`.
    """
    modpath = module.__name__.split(".")

    path: Path
    if stubdir:
        path = stubdir if isinstance(stubdir, Path) else Path(stubdir)
        path = path.joinpath(*modpath[0:-1])
        os.makedirs(path, exist_ok=True)
    elif module.__file__:
        path = Path(module.__file__).parent
    else:
        raise ValueError(f"Directory to output stub file could not be determined.")

    path = path.joinpath(f"{modpath[-1]}.pyi")

    pyi = render_models(models, dialect, mixins, testing, is_async=is_async)

    with open(path, "w") as f:
        for line in pyi:
            f.write(line)
            f.write('\n')


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
    parser.add_argument("module", help="The module where the model types are declared.")
    parser.add_argument("--async", help="The name of the function or method to obtain an asynchronous connection.", dest="async_")

    args = parser.parse_args()

    driver = importlib.import_module(args.driver)

    dialect_module = f"pyracmon.dialect.{args.dialect}"
    dialect = importlib.import_module(dialect_module)

    module = importlib.import_module(args.module)

    if connector_name := args.async_:
        connector = driver
        for n in connector_name.split("."):
            connector = getattr(connector, n)
        async def run_async():
            db = await aconnect(connector, args.dsn, api=args.driver)
            await declare_models_async(dialect, db, module, testing=True, write_stub=True)
        asyncio.run(run_async())
    else:
        db = connect(driver, args.dsn)
        declare_models(dialect, db, module, testing=True, write_stub=True)


if __name__ == "__main__":
    main()