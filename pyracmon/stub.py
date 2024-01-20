"""
This module exports functions to output type stub of model types.
"""
from keyword import iskeyword
import inspect
import os
from pathlib import Path
import types
from typing import Union, get_origin, get_args
from typing_extensions import dataclass_transform
from pyracmon.model import Model
from pyracmon.mixin import CRUDMixin
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.testing import TestingMixin


default_imports = [
    ("typing", ["Any", "Optional"]),
    ("pyracmon", ["Model", "CRUDMixin"]),
    ("pyracmon.model_graph", ["GraphEntityMixin"]),
    ("pyracmon.stub", ["ModelTransform"]),
    ("pyracmon.testing", ["TestingMixin"]),
]


@dataclass_transform(kw_only_default=True)
class ModelTransform:
    pass


def render_models(
    models: list[type[Model]],
    dialect: types.ModuleType,
    mixins: list[type],
    testing: bool = False,
) -> list[str]:
    """
    Generate lines of type stub file (.pyi).

    Args:
        models: List of model types.
        dialect: Dialect type of DB.
        mixins: Mixin types used to declare model types.
    Returns:
        Lines of type stub file.
    """
    lines = []

    super_types: list[str] = []
    additional_imports: dict[str, set[str]] = {}

    for mx in mixins + dialect.mixins:
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

    base_mixins = [CRUDMixin, GraphEntityMixin, ModelTransform, Model]
    if testing:
        base_mixins[0:0] = [TestingMixin]
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
                ct = f"Optional[{ct}]"
            if c.name.isidentifier() and not iskeyword(c.name):
                lines.append(f"    {c.name}: {ct} = ...")
            else:
                lines.append(f"#    {c.name}: {ct} = ...")

    return lines


def output_stub(
    stubdir: Union[str, Path, None],
    module: types.ModuleType,
    models: list[type[Model]],
    dialect: types.ModuleType,
    mixins: list[type],
    testing: bool = False,
):
    """
    Output type stub file (.pyi) into the specified location.

    Args:
        stubdir: Directory to output. If `None` , stub file will be output in the same directory of the module.
        module: Module where model types are declared.
        models: Model types.
        dialect: Dialect type of DB.
        mixins: Mixin types used to declare model types.
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

    pyi = render_models(models, dialect, mixins, testing)

    with open(path, "w") as f:
        for line in pyi:
            f.write(line)
            f.write('\n')