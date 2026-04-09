"""
This module provides mixin type which supplies each model type various DB operations as class methods.
"""
from collections.abc import Mapping, Sequence, Callable
from functools import reduce
from typing import Any, Optional, TypeVar, Union, Literal, cast, overload, Protocol, TYPE_CHECKING
from typing_extensions import Self
from .connection import Connection
from .dbapi import Cursor
from .model import Meta, Column, Record, parse_pks, check_columns, model_values, extract_pks
from .select import SelectMixin, AliasedColumn, read_row
from .query import Q, Expression, Conditional, where
from .clause import ORDER, ranged_by, order_by, values
from .util import key_to_index, Qualifier, PKS


if TYPE_CHECKING:
    class CRUDInternalMeta(Meta):
        @classmethod
        def last_sequences(cls, db: Connection, num: int) -> list[tuple[Column, int]]: ...

        @classmethod
        def support_returning(cls, db: Connection) -> bool: ...
else:
    class CRUDInternalMeta:
        pass


M = TypeVar('M', bound='CRUDMixin')
C = TypeVar('C', bound=Union[str, AliasedColumn], covariant=True)


class CRUDMixin(SelectMixin, CRUDInternalMeta):
    """
    Default mixin providing class methods available on all model types.

    Every method takes the DB connection object as its first argument.
    Following arguments are defined in several methods commonly.

    - `pks`
        - Names and values of all primary key columns in form of `dict` .
        - A primary key value. This form is allowed when the table has just one primary key column.
        - e.g. If the table has a single primary key `id` of int, `1` is available to spcecify the row of `id = 1` .
        - e.g. If the table has multiple primary keys `intid` and `strid`, `dict(intid=1, strid="abc")` is a valid argument.
    - `record`
        - A model object or a mapping from column name to its value, which corresponds to a table row.
        - Only columns contained in the record is affected by the operation.
        - e.g. When `dict(c1=1, c2="abc")` is passed for insertion, only `c1` and `c2` are set in INSERT query.
        - e.g. For update, only the columns will be updated. Other columns are not affected.
    - `condition`
        - Query condition which will compose WHERE clause.
        - `pyracmon.query.Q` is a factory class to create condition object.
        - When `None` is passed, all rows are subject to the operation.
    - `qualifier`
        - A mapping from column name to a function which qualifies a placeholder passed by an argument.
        - Detail of qualifier function is described below.
    - `lock`
        - This is reserved argument for locking statement but works just as the postfix of the query currently.
        - The usage will be changed in future version.

    Qualifier function is used typically to convert or replace placeholder marker in insert/update query.
    By default, those queries contain markers like `insert into t (c1, c2) values (?, ?)` (`Q` parameter style).
    We need sometimes qualify markers to apply DB function, calculation, type cast and so on. This feature enables them like below.

    ```python
    t.insert(db, dict(c1=1, c2=None), dict(c1=lambda x: f"{x}+1", c2=lambda x: "now()"))
    # SQL: INSERT INTO t (c1, c2) VALUES (?+1, now())
    ```

    Be aware that when model object is passed, its column values may differ from actual values in DB after query.
    """
    @classmethod
    def count(cls, db: Connection, condition: Conditional = Q.of()) -> int:
        """
        Count rows which satisfies the condition.

        ```python
        t.count(db, Q.eq(c1=1))
        # SQL: SELECT COUNT(*) FROM t WHERE c1 = 1
        ```

        Args:
            db: DB connection.
            condition: Query condition.
        Returns:
            The number of rows.
        """
        wc, wp = where(condition)
        c = db.stmt().execute(f"SELECT COUNT(*) FROM {cls.name}{_spacer(wc)}", *wp)
        return c.fetchone()[0] # type: ignore

    @classmethod
    def fetch(cls: type[M], db: Connection, pks: PKS, lock: Optional[Any] = None) -> Optional[M]:
        """
        Fetch a record by primary key(s).

        ```python
        t.fetch(db, 1)
        # SQL: SELECT * FROM t WHERE id = 1
        ```

        Args:
            db: DB connection.
            pks: Primary key value(s).
            lock: Locking statement.
        Returns:
            A model object if exists, otherwise `None`.
        """
        cols, vals = parse_pks(cls, pks)
        cond = Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])
        wc, wp = where(cond)
        s = cls.select()
        c = db.stmt().execute(f"SELECT {s} FROM {cls.name}{_spacer(wc)}{_spacer(lock)}", *wp)
        row = c.fetchone()
        return read_row(row, *s)[0] if row else None

    @classmethod
    def fetch_many(cls: type[M], db: Connection, seq_pks: Sequence[PKS], lock: Optional[Any] = None, /, per_page: int = 1000) -> list[M]:
        """
        Fetch a record by sequence of primary key(s).

        This method simply concatenates equality conditions on primary key by OR operator.

        ```python
        t.fetch_many(db, [1, 2, 3])
        # SQL: SELECT * FROM t WHERE id = 1 OR id = 2 OR id = 3
        ```

        Args:
            db: DB connection.
            seq_pks: Sequence of primary key value.
            lock: Locking statement.
            per_page: Maximum number of keys for an execution of query.
        Returns:
            Model objects in the same order as passed sequence.
        """
        res = []
        index = 0
        while index < len(seq_pks):
            ordered_pks = []
            cond = Q.of()
            for pks in seq_pks[index:index+per_page]:
                cols, vals = parse_pks(cls, pks)
                ordered_pks.append(tuple(v for v in vals))
                cond |= Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])
            wc, wp = where(cond)
            s = cls.select()
            c = db.stmt().execute(f"SELECT {s} FROM {cls.name}{_spacer(wc)}{_spacer(lock)}", *wp)

            record_map = {}
            for r in [read_row(row, *s)[0] for row in c.fetchall()]:
                pk_values = {c.name:v for c, v in r if c.pk}
                record_map[tuple([v for _, v in check_columns(cls, pk_values, lambda c: c.pk, True)])] = r

            res.extend([record_map[k] for k in ordered_pks if k in record_map])
            index += per_page

        return res

    @classmethod
    def fetch_where(
        cls: type[M],
        db: Connection,
        condition: Conditional = Q.of(),
        orders: Mapping[C, ORDER] = {},
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        lock: Optional[Any] = None,
    ) -> list[M]:
        """
        Fetch records which satisfy the condition.

        ```python
        t.fetch_where(db, Q.eq(c1=1), dict(c2=True), 10, 5)
        # SQL: SELECT * FROM t WHERE c1 = 1 ORDER BY c2 ASC LIMIT 10 OFFSET 5
        ```

        Args:
            db: DB connection.
            condition: Query condition.
            orders: Ordering specification where key is column name and value denotes whether the order is ascending or not.
            limit: Maximum nuber of rows to fetch. If `None`, all rows are returned.
            offset: The number of rows to skip.
            lock: Locking statement.
        Returns:
            Model objects.
        """
        wc, wp = where(condition)
        rc, rp = ranged_by(limit, offset)
        s = cls.select()
        c = db.stmt().execute(f"SELECT {s} FROM {cls.name}{_spacer(wc)}{_spacer(order_by(orders))}{_spacer(rc)}{_spacer(lock)}", *(wp + rp))
        return [read_row(row, *s)[0] for row in c.fetchall()]

    @classmethod
    def fetch_one(
        cls: type[M],
        db: Connection,
        condition: Conditional = Q.of(),
        lock: Optional[Any] = None,
    ) -> Optional[M]:
        """
        Fetch a record which satisfies the condition.

        `ValueError` raises When multiple records are found.
        Use this method for queries which certainly returns a single row, such as search by unique key.

        ```python
        t.fetch_one(db, Q.eq(c1=1))
        # SQL: SELECT * FROM t WHERE c1 = 1
        ```

        Args:
            db: DB connection.
            condition: Query condition.
            lock: Locking statement.
        Returns:
            Model objects If exists, otherwise `None`.
        """
        rs = cls.fetch_where(db, condition, lock=lock)

        if not rs:
            return None
        elif len(rs) == 1:
            return rs[0]
        else:
            raise ValueError(f"{len(rs)} records are found on the invocation of fetch_one().")

    @classmethod
    def _insert_sql(cls: type[M], record: Union[M, dict[str, Any]], qualifier: Mapping[str, Qualifier] = {}) -> tuple[str, list[str], list[Any]]:
        model: M = record if isinstance(record, cls) else cls(**cast(dict, record))
        value_dict = model_values(cls, model)
        check_columns(cls, value_dict)
        cols, vals = list(value_dict.keys()), list(value_dict.values())
        ordered_qs = key_to_index(qualifier, cols)

        def exp(v):
            return lambda i: v

        if any(isinstance(v, Expression) for v in vals):
            key_gen = []
            org_vals = vals
            vals = []
            for v in org_vals:
                if isinstance(v, Expression):
                    key_gen.append(exp(v))
                    vals.extend(v.params)
                else:
                    key_gen.append(lambda i: None)
                    vals.append(v)
            values_clause = values(key_gen, 1, ordered_qs)
        else:
            values_clause = values(len(cols), 1, ordered_qs)

        return f"INSERT INTO {cls.name} ({', '.join(cols)}) VALUES {values_clause}", cols, vals

    @classmethod
    def insert(
        cls: type[M],
        db: Connection,
        record: Union[M, dict[str, Any]],
        qualifier: Mapping[str, Qualifier] = {},
        /,
        returning: bool = False,
    ) -> M:
        """
        Insert a record.

        If `returning` is `True` and the DBMS supports **RETURNING** clause,
        returned model object contains comple and correct column values.
        Otherwise, auto incremental value is set to the returned model object
        but other column values generated inside DBMS such as default value are not set.

        ```python
        t.insert(db, dict(c1=1, c2=2))
        # SQL: INSERT INTO t (c1, c2) VALUES (1, 2)
        ```

        Args:
            db: DB connection.
            record: Object contains column values.
            qualifier: Functions qualifying placeholder markers.
            returning: Flag to return inserted record with complete and correct column values.
        Returns:
            Model of inserted record.
        """
        model: M = record if isinstance(record, cls) else cls(**cast(dict, record))
        sql, _, vals = cls._insert_sql(record, qualifier)

        if returning:
            if cls.support_returning(db):
                c = db.stmt().execute(f"{sql} RETURNING *", *vals)
                s = cls.select()
                return read_row(c.fetchone(), *s)[0]
            else:
                # REVIEW
                # Inserted row can't be specified from the table where no primary keys are defined .
                pass

        db.stmt().execute(sql, *vals)
        for c, v in cls.last_sequences(db, 1):
            setattr(model, c.name, v)
        return model

    @classmethod
    @overload
    def insert_many(cls: type[M], db: Connection, records: list[Union[M, dict[str, Any]]], qualifier: Mapping[str, Qualifier] = {},
                    /, returning: Literal[False] = False) -> list[M]: ...
    @classmethod
    @overload
    def insert_many(cls: type[M], db: Connection, records: list[Union[M, dict[str, Any]]], qualifier: Mapping[str, Qualifier] = {},
                    /, returning: Literal[True] = True) -> list[M]: ...
    @classmethod
    def insert_many(
        cls: type[M],
        db: Connection,
        records: list[Union[M, dict[str, Any]]],
        qualifier: Mapping[str, Qualifier] = {},
        /,
        returning: bool = False,
    ):
        """
        Insert records.

        If `returning` is `True` and the DBMS supports **RETURNING** clause,
        returned model object contains comple and correct column values.
        Otherwise, auto incremental value is set to the returned model object
        but other column values generated inside DBMS such as default value are not set.

        Args:
            db: DB connection.
            record: Object contains column values.
            qualifier: Functions qualifying placeholder markers.
            returning: Flag to return inserted records with complete and correct column values.
        Returns:
            Models of inserted records or cursor.
        """
        if len(records) == 0:
            return []

        models: list[M] = [r if isinstance(r, cls) else cls(**cast(dict, r)) for r in records]

        seq_of_params = []

        sql, cols, params = cls._insert_sql(models[0], qualifier)

        cols = set(cols)
        seq_of_params.append(params)

        for m in models[1:]:
            value_dict = model_values(cls, m)
            check_columns(cls, value_dict, lambda c: c.name in cols, requires_all=True)
            # REVIEW:
            # The consistency among columns where expression is set is not checked.
            _, _, params = cls._insert_sql(m, qualifier)
            seq_of_params.append(params)

        db.stmt().executemany(sql, seq_of_params)
        num = len(records)
        for c, v in cls.last_sequences(db, num):
            for i, m in enumerate(models):
                setattr(m, c.name, v - (num - i - 1))

        if returning:
            seq_pks = [extract_pks(cls, m) for m in models]
            return cls.fetch_many(db, seq_pks)
        else:
            return models

    @classmethod
    def _update_sql(cls, record: Record, condition: Conditional, qualifier: Mapping[str, Qualifier] = {}, allow_all: bool = True) -> tuple[str, list[str], list[Any]]:
        value_dict = model_values(cls, record, excludes_pk=True)
        check_columns(cls, value_dict)
        cols, vals = list(value_dict.keys()), list(value_dict.values())
        ordered_qs = key_to_index(qualifier, cols)

        def set_col(acc, icv):
            i, (c, v) = icv
            if isinstance(v, Expression):
                clause = f"{c} = {ordered_qs.get(i, lambda x:x)(v.expression)}"
                params = v.params
            else:
                clause = f"{c} = {ordered_qs.get(i, lambda x:x)('$_')}"
                params = [v]
            acc[0].append(clause)
            acc[1].extend(params)
            return acc

        setters, params = reduce(set_col, enumerate(zip(cols, vals)), ([], []))

        wc, wp = where(condition)
        if wc == "" and not allow_all:
            raise ValueError("Update query to update all records is not allowed.")

        return f"UPDATE {cls.name} SET {', '.join(setters)}{_spacer(wc)}", cols, params + wp

    @classmethod
    @overload
    def update(cls, db: Connection, pks: PKS, record: Record, qualifier: Mapping[str, Qualifier] = {},
               /, returning: Literal[False] = False) -> bool: ...
    @classmethod
    @overload
    def update(cls: type[M], db: Connection, pks: PKS, record: Record, qualifier: Mapping[str, Qualifier] = {},
               /, returning: Literal[True] = True) -> Optional[M]: ...
    @classmethod
    def update(
        cls: type[M],
        db: Connection,
        pks: PKS,
        record: Record,
        qualifier: Mapping[str, Qualifier] = {},
        /,
        returning: bool = False,
    ):
        """
        Update a record by primary key(s).

        This method only updates columns which are found in `record` except for primary key(s).

        ```python
        t.update(db, 1, dict(c1=1, c2=2))
        # SQL: UPDATE t SET c1 = 1, c2 = 2 WHERE id = 1
        ```

        Args:
            db: DB connection.
            pks: Primary key value(s).
            record: Object contains column values.
            qualifier: Functions qualifying placeholder markers.
            returning: Flag to return updated records with complete and correct column values.
        Returns:
            Whether the record exists and updated or updated record model.
        """
        cols, vals = parse_pks(cls, pks)
        condition = Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])
        if returning:
            if cls.support_returning(db):
                models = cls.update_where(db, record, condition, qualifier, returning=True)
                return models[0] if models else None
            else:
                models = cls.update_where(db, record, condition, qualifier, returning=False)
                return cls.fetch(db, pks)
        else:
            return cls.update_where(db, record, condition, qualifier, returning=False) == 1

    @classmethod
    @overload
    def update_many(cls, db: Connection, records: Sequence[Record], qualifier: Mapping[str, Qualifier] = {},
               /, returning: Literal[False] = False) -> int: ...
    @classmethod
    @overload
    def update_many(cls: type[M], db: Connection, records: Sequence[Record], qualifier: Mapping[str, Qualifier] = {},
               /, returning: Literal[True] = True) -> list[M]: ...
    @classmethod
    def update_many(
        cls,
        db: Connection,
        records: Sequence[Record],
        qualifier: Mapping[str, Qualifier] = {},
        /,
        returning: bool = False,
    ):
        """
        Update records by set of primary key(s).

        This method invokes on `executemany` defined in DB-API 2.0.
        Whether it is optimized compared to `execute` depends on DB driver.

        Args:
            db: DB connection.
            record: Sequence of objects contains column values.
            qualifier: Functions qualifying placeholder markers.
            returning: Flag to return updated records with complete and correct column values.
        Returns:
            The number of affected rows or updated records.
        """
        if len(records) == 0:
            return [] if returning else 0

        keys = {c.name for c in cls.columns if c.pk}
        if len(keys) == 0:
            raise ValueError(f"update_many is not available because {cls} does not have primary key columns.")

        def classify(acc: tuple[dict[str, Any], dict[str, Any]], cv: tuple[str, Any]):
            if cv[0] in keys:
                acc[0][cv[0]] = cv[1]
            else:
                acc[1][cv[0]] = cv[1]
            return acc

        seq_of_values: list[tuple[dict[str, Any], dict[str, Any]]] = []
        target_columns: Optional[set[str]] = None

        for vs in [model_values(cls, r, excludes_pk=False) for r in records]:
            if not keys < vs.keys():
                raise ValueError(f"Every row must contain values of all primary keys and at least one update column value.")
            pks, rec = reduce(classify, vs.items(), ({}, {}))
            if target_columns is None:
                check_columns(cls, rec)
                target_columns = set(rec.keys())
            else:
                check_columns(cls, rec, lambda c: c.name in target_columns, True) # type: ignore
            seq_of_values.append((pks, rec))

        sql_first = ""
        seq_of_params: list[list[Any]] = []

        for pks, rec in seq_of_values:
            cols, vals = parse_pks(cls, pks)
            condition = Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])

            sql, _, params = cls._update_sql(rec, condition, qualifier)
            if not sql_first:
                sql_first = sql
            seq_of_params.append(params)

        if returning:
            db.stmt().executemany(f"{sql_first}", seq_of_params)
            return cls.fetch_many(db, [pks for pks, _ in seq_of_values])
        else:
            return db.stmt().executemany(sql_first, seq_of_params).rowcount

    @classmethod
    @overload
    def update_where(cls, db: Connection, record: Record, condition: Conditional, qualifier: Mapping[str, Qualifier] = {},
                     /, returning: Literal[False] = False, allow_all: bool = False) -> int: ...
    @classmethod
    @overload
    def update_where(cls: type[M], db: Connection, record: Record, condition: Conditional, qualifier: Mapping[str, Qualifier] = {},
                     /, returning: Literal[True] = True, allow_all: bool = False) -> list[M]: ...
    @classmethod
    def update_where(
        cls,
        db: Connection,
        record: Record,
        condition: Conditional,
        qualifier: Mapping[str, Qualifier] = {},
        /,
        returning: bool = False,
        allow_all: bool = True,
    ):
        """
        Update records which satisfy the condition.

        ```python
        t.update(db, dict(c2=2), Q.eq(c1=1))
        # SQL: UPDATE t SET c2 = 2 WHERE c1 = 1
        ```

        Args:
            db: DB connection.
            record: Object contains column values.
            condition: Query condition.
            qualifier: Functions qualifying placeholder markers.
            returning: Flag to return updated records with complete and correct column values.
            allow_all: If `False`, empty condition raises `ValueError`.
        Returns:
            The number of affected rows or updated records.
        """
        sql, _, params = cls._update_sql(record, condition, qualifier, allow_all)

        if returning:
            if cls.support_returning(db):
                c = db.stmt().execute(f"{sql} RETURNING *", *params)
                s = cls.select()
                return [read_row(row, *s)[0] for row in c.fetchall()]
            else:
                raise NotImplementedError(f"RETURNING is not supported and there is no way to fetch updated rows exactly.")
        else:
            c = db.stmt().execute(sql, *params)

            return c.rowcount

    @classmethod
    @overload
    def delete(cls, db: Connection, pks: PKS, /, returning: Literal[False] = False) -> bool: ...
    @classmethod
    @overload
    def delete(cls: type[M], db: Connection, pks: PKS, /, returning: Literal[True] = True) -> Optional[M]: ...
    @classmethod
    def delete(cls: type[M], db: Connection, pks: PKS, /, returning: bool = False):
        """
        Delete a record by primary key(s).

        ```python
        t.delete(db, 1)
        # SQL: DELETE FROM t WHERE id = 1
        ```

        Args:
            db: DB connection.
            pks: Primary key value(s).
            returning: Flag to return deleted record if any.
        Returns:
            Whether the record exists and deleted or delete record if any.
        """
        cols, vals = parse_pks(cls, pks)

        if returning:
            models = cls.delete_where(db, Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)]), returning=True)
            return models[0] if models else None
        else:
            return cls.delete_where(db, Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])) == 1

    @classmethod
    @overload
    def delete_many(cls, db: Connection, pks: Union[Sequence[PKS], Sequence[Record]], /, returning: Literal[False] = False) -> int: ...
    @overload
    @classmethod
    def delete_many(cls: type[M], db: Connection, pks: Union[Sequence[PKS], Sequence[Record]], /, returning: Literal[True] = True) -> list[M]: ...
    @classmethod
    def delete_many(cls: type[M], db: Connection, pks: Union[Sequence[PKS], Sequence[Record]], /, returning: bool = False):
        """
        Delete a records by set of primary key(s).

        This method invokes on `executemany` defined in DB-API 2.0.
        Whether it is optimized compared to `execute` depends on DB driver.

        Args:
            db: DB connection.
            pks: Primary keys or objects each of which contains values of all primary keys.
            returning: Flag to return deleted records.
        Returns:
            The number of affected rows or deleted records.
        """
        if len(pks) == 0:
            return None

        seq_of_pks: list[dict[str, Any]] = []
        for rec in pks:
            if isinstance(rec, (dict, cls)):
                seq_of_pks.append(extract_pks(cls, rec))
            else:
                cols, vals = parse_pks(cls, rec)
                seq_of_pks.append(dict(zip(cols, vals)))

        condition = Conditional.all([Q.eq(**{c: v}) for c, v in seq_of_pks[0].items()])
        wc, wp = where(condition)

        sql = f"DELETE FROM {cls.name}{_spacer(wc)}"
        seq_of_params: list[list[Any]] = [wp]

        for v in seq_of_pks[1:]:
            condition = Conditional.all([Q.eq(**{c: v}) for c, v in v.items()])
            _, wp = where(condition)
            seq_of_params.append(wp)

        if returning:
            models = cls.fetch_many(db, seq_of_pks)
            db.stmt().executemany(sql, seq_of_params)
            return models
        else:
            return db.stmt().executemany(sql, seq_of_params).rowcount

    @classmethod
    @overload
    def delete_where(cls, db: Connection, condition: Conditional, /, returning: Literal[False] = False, allow_all: bool = True) -> int: ...
    @classmethod
    @overload
    def delete_where(cls: type[M], db: Connection, condition: Conditional, /, returning: Literal[True] = True, allow_all: bool = True) -> list[M]: ...
    @classmethod
    def delete_where(cls: type[M], db: Connection, condition: Conditional, /, returning: bool = False, allow_all: bool = True):
        """
        Delete records which satisfy the condition.

        ```python
        t.delete(db, Q.eq(c1=1))
        # SQL: DELETE FROM t WHERE c1 = 1
        ```

        Args:
            db: DB connection.
            condition: Query condition.
            returning: Flag to return deleted records.
            allow_all: If `False`, empty condition raises `ValueError`.
        Returns:
            The number of affected rows or deleted records.
        """
        wc, wp = where(condition)
        if wc == "" and not allow_all:
            raise ValueError("Delete query to delete all records is not allowed.")

        sql = f"DELETE FROM {cls.name}{_spacer(wc)}"

        if returning:
            if cls.support_returning(db):
                c = db.stmt().execute(f"{sql} RETURNING *", *wp)
                return [read_row(row, *cls.select())[0] for row in c.fetchall()]
            else:
                current = cls.fetch_where(db, condition)
                c = db.stmt().execute(sql, *wp)
                return current
        else:
            return db.stmt().execute(sql, *wp).rowcount

    @classmethod
    def last_sequences(cls, db: Connection, num: int) -> list[tuple[Column, int]]:
        """
        Returns the sequential (auto incremental) values of a table generated by the latest insertion.

        Result contains every sequential columns and their values.
        When the latest query inserts multiple rows, only the last (= biggest) value is returned.

        This method should be overridden by another mixin class defined in dialect module.

        Args:
            db: DB connection.
            num: The number of records inserted by the latest query.
        Returns:
            List of pairs of column and its values.
        """
        return []

    @classmethod
    def support_returning(cls, db: Connection) -> bool:
        """
        Checks whehter this DBMS support **RETURNING** clause or not.

        Args:
            db: DB connection.
        Returns:
            Whehter this DBMS support **RETURNING** clause or not.
        """
        return False


def _spacer(s):
    return (" " + str(s)) if s else ""