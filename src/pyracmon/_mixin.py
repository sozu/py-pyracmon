from collections.abc import Mapping, Callable, Sequence
from itertools import repeat
from typing import Any, cast
from .model import IModel, Record, Column, model_values, check_columns, extract_pks, parse_pks
from .query import Expression, Conditional, Q, where
from .util import key_to_index, Qualifier, PKS


def _insert[M: IModel](model: type[M], expressions: list[tuple[str, str]], /, length: int = 1) -> str:
    values = ', '.join(repeat(f"({', '.join(e for _, e in expressions)})", length))
    return f"INSERT INTO {model.name} ({', '.join(c for c, _ in expressions)}) VALUES {values}"


def _update[M: IModel](model: type[M], expressions: list[tuple[str, str]]) -> str:
    return f"UPDATE {model.name} SET {', '.join(f'{c} = {e}' for c, e in expressions)}"


def _render[M: IModel](
    model: type[M],
    record: Record,
    qualifier: Mapping[str, Qualifier],
    /,
    condition: Callable[[Column], bool] = lambda c: True,
    excludes_pk: bool = False,
    requires_all: bool = False,
) -> tuple[M, list[tuple[str, str]], list[Any]]:
    record = record if isinstance(record, model) else model(**cast(dict, record))

    # Extract column names and values and check if they are valid for the model.
    column_values: dict[str, Any] = model_values(model, record, excludes_pk=excludes_pk)
    check_columns(model, column_values, condition=condition, requires_all=requires_all)

    # Get the order of columns for each qualifier.
    ordered_qualifiers: dict[int, Qualifier] = key_to_index(qualifier, list(column_values.keys()))

    # Create a list of expressions for each column, expression, and parameters for the expression.
    expressions: list[tuple[str, str]] = []
    params: list[Any] = []
    for i, (c, v) in enumerate(column_values.items()):
        q: Callable[[str], str] = ordered_qualifiers.get(i, lambda x: x)
        exp: str = ""

        if isinstance(v, Expression):
            exp = q(v.expression)
            params.extend(v.params)
        else:
            exp = q("${_}")
            params.append(v)

        expressions.append((c, exp))

    return record, expressions, params


def _render_many[M: IModel](
    model: type[M],
    records: Sequence[Record],
    qualifier: Mapping[str, Qualifier],
    /,
    excludes_pk: bool = False,
) -> tuple[list[M], list[tuple[str, str]], list[list[Any]]]:
    if len(records) == 0:
        return [], [], []

    first = records[0]
    m, expressions, ps = _render(model, first, qualifier, excludes_pk=excludes_pk)

    colset = {c for c, _ in expressions}

    params: list[list[Any]] = [ps]
    models: list[M] = [m]
    condition: Callable[[Column], bool] = lambda c: c.name in colset

    for r in records[1:]:
        m, _, ps = _render(model, r, qualifier, condition=condition, excludes_pk=excludes_pk, requires_all=True)
        models.append(m)
        params.append(ps)

    return models, expressions, params


def _set_sequences[M: IModel](records: list[M], sequences: list[tuple[Column, int]]) -> None:
    num = len(records)
    for c, v in sequences:
        for i, r in enumerate(records):
            setattr(r, c.name, v - (num - i - 1))


def _pk_condition[M: IModel](model: type[M], value: Record | PKS) -> tuple[PKS, Conditional]:
    pks: PKS
    if isinstance(value, (dict, model)):
        # Record
        pks = extract_pks(model, value)
    else:
        pks = value

    cols, vals = parse_pks(model, pks)

    return pks, Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])


def _pk_condition_many[M: IModel](model: type[M], values: Sequence[Record | PKS]) -> tuple[str, list[PKS], list[Any], Conditional]:
    pks, condition = _pk_condition(model, values[0])
    wc, wp = where(condition)

    all_params = [wp]
    all_pks = [pks]

    for v in values[1:]:
        pks, condition = _pk_condition(model, v)
        _, wp = where(condition)

        all_params.append(wp)
        all_pks.append(pks)

    return wc, all_pks, all_params, condition