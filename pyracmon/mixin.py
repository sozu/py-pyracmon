from functools import reduce
from typing import *
from .connection import Connection
from .model import *
from .select import *
from .query import *
from .util import key_to_index, Qualifier


class CRUDMixin(SelectMixin):
    """
    Default mixin providing class methods available on all model types.

    Every method takes the DB connection object as its first argument.

    Following arguments are shared in some methods. Some of them have variations in their forms which are available commonly in every method.

    **pks**

    - `Dict[str, Any]`
        - Values of multiple primary keys. Key of `dict` corresponds to column name.
    - `Any`
        - A value of primary key. 

    **record**
    
    - `Model`
        - A model object representing a record. Only existing columns affects the operation.
    - `Dict[str, Any]`
        - A `dict` where column name is mapped to its value. Only existing columns affects the operation.

    **condition**

    - `Conditional`
        - Query condition to select rows to fetch, update or delete.

    **qualifier**

    - `Dict[str, Callable[[str], str]]`
        - Functions to convert query expressions for columns. Each function takes default expression and should return actual expression.

    **lock**

    - `Any`
        - This is reserved argument for locking statement but works just as the postfix of the query currently.
        - The usage will be changed in future version.

    Qualifier is used typically to convert or replace placeholder marker in insert/update query.
    By default, those queries contain markers like `insert into t (c1, c2) values (?, ?)` (`Q` parameter style).
    We need sometimes qualify markers to apply DB function, calculation, type cast and so on. This feature enables them like below.

    >>> t.insert(db, dict(c1=1, c2=None), dict(c1=lambda x: f"{x}+1", c2=lambda x: "now()"))
    >>> # SQL: INSERT INTO t (c1, c2) VALUES (?+1, now())

    Be aware that when model object is used for the second argument, its column values may differ from actual values in DB after query.
    """
    @classmethod
    def count(cls, db: Connection, condition: Conditional = Q.of()) -> int:
        """
        Count rows which satisfies the condition.

        >>> t.count(db, Q.eq(c1=1))
        >>> # SQL: SELECT COUNT(*) FROM t WHERE c1 = 1

        Args:
            db: DB connection.
            condition: Query condition.
        Returns:
            The number of rows.
        """
        wc, wp = where(condition)
        c = db.stmt().execute(f"SELECT COUNT(*) FROM {cls.name}{_spacer(wc)}", *wp)
        return c.fetchone()[0]

    @classmethod
    def fetch(cls, db: Connection, pks: PKS, lock: Optional[Any] = None) -> Optional[Model]:
        """
        Fetch a record by primary key(s).

        >>> t.fetch(db, 1)
        >>> # SQL: SELECT * FROM t WHERE id = 1

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
        return read_row(row, s)[0] if row else None

    @classmethod
    def fetch_where(
        cls,
        db: Connection,
        condition: Conditional = Q.of(),
        orders: Dict[str, bool] = {},
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        lock: Optional[Any] = None,
    ) -> List[Model]:
        """
        Fetch records which satisfy the condition.

        >>> t.fetch_where(db, Q.eq(c1=1), dict(c2=True), 10, 5)
        >>> # SQL: SELECT * FROM t WHERE c1 = 1 ORDER BY c2 ASC LIMIT 10 OFFSET 5

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
        return [read_row(row, s)[0] for row in c.fetchall()]

    @classmethod
    def fetch_one(
        cls,
        db: Connection,
        condition: Conditional = Q.of(),
        lock: Optional[Any] = None,
    ) -> Optional[Model]:
        """
        Fetch a record which satisfies the condition.

        `ValueError` raises When multiple records are found.
        Use this method for queries which certainly returns a single row, such as search by unique key.

        >>> t.fetch_one(db, Q.eq(c1=1)), 5)
        >>> # SQL: SELECT * FROM t WHERE c1 = 1

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
    def insert(cls, db: Connection, record: Record, qualifier: Qualifier = {}) -> Model:
        """
        Insert a record.

        Returned model object contains auto incremental column even if they are not set beforehand.
        On the contrary, default value generated in database side is not set.

        >>> t.insert(db, dict(c1=1, c2=2))
        >>> # SQL: INSERT INTO t (c1, c2) VALUES (1, 2)

        Args:
            db: DB connection.
            record: Object contains column values.
            qualifier: Functions qualifying placeholder markers.
        Returns:
            Model of inserted record.
        """
        record = record if isinstance(record, cls) else cls(**record)
        value_dict = model_values(cls, record)
        check_columns(cls, value_dict)
        cols, vals = list(value_dict.keys()), list(value_dict.values())
        qualifier = key_to_index(qualifier, cols)

        db.stmt().execute(f"INSERT INTO {cls.name} ({', '.join(cols)}) VALUES {values(len(cols), 1, qualifier)}", *vals)

        for c, v in cls.last_sequences(db, 1):
            setattr(record, c.name, v)

        return record

    @classmethod
    def update(cls, db: Connection, pks: PKS, record: Record, qualifier: Qualifier = {}) -> bool:
        """
        Update a record by primary key(s).

        This method only updates columns which are found in `record` except for primary key(s).

        >>> t.update(db, 1, dict(c1=1, c2=2))
        >>> # SQL: UPDATE t SET c1 = 1, c2 = 2 WHERE id = 1

        Args:
            db: DB connection.
            pks: Primary key value(s).
            record: Object contains column values.
            qualifier: Functions qualifying placeholder markers.
        Returns:
            Whether the record exists and updated.
        """
        cols, vals = parse_pks(cls, pks)
        return cls.update_where(db, record, Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)]), qualifier) == 1

    @classmethod
    def update_where(
        cls,
        db: Connection,
        record: Record,
        condition: Conditional,
        qualifier: Qualifier = {},
        allow_all: bool = True,
    ) -> int:
        """
        Update records which satisfy the condition.

        >>> t.update(db, dict(c2=2), Q.eq(c1=1))
        >>> # SQL: UPDATE t SET c2 = 2 WHERE c1 = 1

        Args:
            db: DB connection.
            record: Object contains column values.
            condition: Query condition.
            qualifier: Functions qualifying placeholder markers.
            allow_all: If `False`, empty condition raises `ValueError`.
        Returns:
            The number of affected rows.
        """
        value_dict = model_values(cls, record, excludes_pk=True)
        check_columns(cls, value_dict)
        cols, vals = list(value_dict.keys()), list(value_dict.values())
        qualifier = key_to_index(qualifier, cols)

        def set_col(acc, icv):
            i, (c, v) = icv
            if isinstance(v, Expression):
                clause = f"{c} = {qualifier.get(i, lambda x:x)(v.expression)}"
                params = v.params
            else:
                clause = f"{c} = {qualifier.get(i, lambda x:x)('$_')}"
                params = [v]
            acc[0].append(clause)
            acc[1].extend(params)
            return acc

        setters, params = reduce(set_col, enumerate(zip(cols, vals)), ([], []))

        wc, wp = where(condition)
        if wc == "" and not allow_all:
            raise ValueError("Update query to update all records is not allowed.")

        c = db.stmt().execute(f"UPDATE {cls.name} SET {', '.join(setters)}{_spacer(wc)}", *(params + wp))

        return getattr(c, "rowcount", None)

    @classmethod
    def delete(cls, db: Connection, pks: PKS) -> bool:
        """
        Delete a record by primary key(s).

        >>> t.delete(db, 1)
        >>> # SQL: DELETE FROM t WHERE id = 1

        Args:
            db: DB connection.
            pks: Primary key value(s).
        Returns:
            Whether the record exists and deleted.
        """
        cols, vals = parse_pks(cls, pks)
        return cls.delete_where(db, Conditional.all([Q.eq(**{c: v}) for c, v in zip(cols, vals)])) == 1

    @classmethod
    def delete_where(cls, db: Connection, condition: Conditional, allow_all: bool = True) -> int:
        """
        Delete records which satisfy the condition.

        >>> t.delete(db, Q.eq(c1=1))
        >>> # SQL: DELETE FROM t WHERE c1 = 1

        Args:
            db: DB connection.
            condition: Query condition.
            allow_all: If `False`, empty condition raises `ValueError`.
        Returns:
            The number of affected rows.
        """
        wc, wp = where(condition)
        if wc == "" and not allow_all:
            raise ValueError("Delete query to delete all records is not allowed.")

        c = db.stmt().execute(f"DELETE FROM {cls.name}{_spacer(wc)}", *wp)

        return getattr(c, "rowcount", None)

    @classmethod
    def last_sequences(cls, db: Connection, num: int) -> List[Tuple[Column, int]]:
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


def _spacer(s):
    return (" " + str(s)) if s else ""