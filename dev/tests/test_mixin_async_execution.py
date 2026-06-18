from typing import NamedTuple, TYPE_CHECKING
import psycopg
import pytest
import pytest_asyncio
from pyracmon import declare_models_async
from pyracmon.connection import AsyncConnection, aconnect
from pyracmon.model import Model
from pyracmon.testing import truncate_async
from pyracmon.dialect.shared import AsyncTruncateMixin
from pyracmon.dialect import postgresql
from pyracmon.mixin_async import *
from .fixtures import COLUMN


if TYPE_CHECKING:
    class m(NamedTuple):
        class t1(Model, AsyncTruncateMixin, AsyncCRUDMixin): c11: int = COLUMN; c12: int = COLUMN; c13: str = COLUMN
        class t2(Model, AsyncTruncateMixin, AsyncCRUDMixin): c21: int = COLUMN; c22: int = COLUMN; c23: str = COLUMN
        class t3(Model, AsyncTruncateMixin, AsyncCRUDMixin): c31: int = COLUMN; c32: int = COLUMN; c33: str = COLUMN
        class t4(Model, AsyncTruncateMixin, AsyncCRUDMixin): c41: int = COLUMN; c42: int = COLUMN; c43: int = COLUMN
else:
    from tests import models as m


async def _connect_postgresql() -> AsyncConnection:
    return await aconnect(
        psycopg.AsyncConnection.connect,
        api=psycopg,
        conninfo = "dbname=pyracmon_test user=postgres password=postgres host=postgres port=5432",
    )


@pytest_asyncio.fixture
async def db():
    db = await _connect_postgresql()

    if 't1' not in dir(m):
        await declare_models_async(postgresql, db, 'tests.models')

    await truncate_async(db, m.t4, m.t3, m.t2, m.t1)
    try:
        await db.stmt().execute("begin")
        yield db
    finally:
        await db.rollback()
        await db.close()


class TestCount:
    async def fixture(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

    @pytest.mark.asyncio
    async def test_count_all(self, db: AsyncConnection):
        await self.fixture(db)

        r = await m.t1.count(db)

        assert r == 3

    @pytest.mark.asyncio
    async def test_count_where(self, db: AsyncConnection):
        await self.fixture(db)

        r = await m.t1.count(db, Q.gt(c12 = 2) & Q.lt(c12 = 4))

        assert r == 1


class TestFetch:
    @pytest.mark.asyncio
    async def test_fetch(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = await m.t1.fetch(db, 2)

        assert r is not None
        assert (r.c11, r.c12, r.c13) == (2, 3, 'def')

    @pytest.mark.asyncio
    async def test_not_found(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = await m.t1.fetch(db, 0)

        assert r is None

    @pytest.mark.asyncio
    async def test_multiple_pks(self, db: AsyncConnection):
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        r = await m.t2.fetch(db, dict(c21=2, c22=3))

        assert r is not None
        assert (r.c21, r.c22, r.c23) == (2, 3, 'def')

    @pytest.mark.asyncio
    async def test_multiple_not_found(self, db: AsyncConnection):
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        r = await m.t2.fetch(db, dict(c21=1, c22=3))

        assert r is None

    @pytest.mark.asyncio
    async def test_mssing_pk(self, db: AsyncConnection):
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        with pytest.raises(ValueError):
            await m.t2.fetch(db, dict(c21=1))


class TestFetchMany:
    @pytest.mark.asyncio
    async def test_fetch(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = await m.t1.fetch_many(db, [1, 3, 5])

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi')]

    @pytest.mark.asyncio
    async def test_duplicate(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = await m.t1.fetch_many(db, [1, 3, 5, 1])

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi'), m.t1(c11=1, c12=2, c13='abc')]

    @pytest.mark.asyncio
    async def test_pks(self, db: AsyncConnection):
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi'), (4, 5, 'jkl'), (5, 6, 'mno')")

        r = await m.t2.fetch_many(db, [dict(c21=3, c22=4), dict(c21=0, c22=0), dict(c21=5, c22=6), dict(c21=1, c22=2)])

        assert [v.c21 for v in r] == [3, 5, 1]

    @pytest.mark.asyncio
    async def test_pages(self, db: AsyncConnection):
        values = ', '.join(f"({v12}, '{v13}')" for v12, v13 in [(i+1, f"c{i+1}") for i in range(50)])
        await db.stmt().execute(f"insert into t1 (c12, c13) values {values}")

        r = await m.t1.fetch_many(db, range(1, 30, 2), per_page=5)

        assert [v.c11 for v in r] == [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]


class TestFetchWhere:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    @pytest.mark.asyncio
    async def test_no_condition(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_where(db)

        assert len(r) == 5
        assert {1, 2, 3, 4, 5} == {v.c11 for v in r}

    @pytest.mark.asyncio
    async def test_with_condition(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_where(db, Q.eq(c12=3))

        assert len(r) == 2
        assert {2, 4} == {v.c11 for v in r}

    @pytest.mark.asyncio
    async def test_orders(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_where(db, orders=dict(c12=True, c11=False))

        assert len(r) == 5
        assert [5, 1, 4, 2, 3] == [v.c11 for v in r]

    @pytest.mark.asyncio
    async def test_limit(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_where(db, orders=dict(c11=True), limit=3, offset=1)

        assert len(r) == 3
        assert [2, 3, 4] == [v.c11 for v in r]

    @pytest.mark.asyncio
    async def test_various(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_where(db, Q.gt(c11=1) & Q.lt(c12=4), orders=dict(c11=True), limit=2, offset=1, lock="FOR UPDATE")

        assert len(r) == 2
        assert [4, 5] == [v.c11 for v in r]


class TestFetchOne:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    @pytest.mark.asyncio
    async def test_singular(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_one(db, Q.eq(c13='jkl'))

        assert r is not None
        assert (r.c11, r.c12, r.c13) == (4, 3, 'jkl')

    @pytest.mark.asyncio
    async def test_empty(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.fetch_one(db, Q.eq(c13='pqr'))

        assert r is None

    @pytest.mark.asyncio
    async def test_multiple(self, db: AsyncConnection):
        await self.prepare(db)

        with pytest.raises(ValueError):
            await m.t1.fetch_one(db, Q.eq(c12=3))


class TestInsert:
    @pytest.mark.asyncio
    async def test_insert(self, db: AsyncConnection):
        r = await m.t2.insert(db, dict(c21=1, c22=2, c23='abc'))

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert await m.t2.count(db) == 1
        assert await m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    @pytest.mark.asyncio
    async def test_insert_model(self, db: AsyncConnection):
        r = await m.t2.insert(db, m.t2(c21=1, c22=2, c23='abc'))

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert await m.t2.count(db) == 1
        assert await m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    @pytest.mark.asyncio
    async def test_insert_returning(self, db: AsyncConnection):
        r = await m.t2.insert(db, dict(c21=1, c22=2, c23='abc'), returning=True)

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert await m.t2.count(db) == 1
        assert await m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    @pytest.mark.asyncio
    async def test_set_pk(self, db: AsyncConnection):
        r = await m.t1.insert(db, dict(c12=2, c13='abc'))

        assert (r.c11, r.c12, r.c13) == (1, 2, 'abc')

        assert await m.t1.count(db) == 1
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    @pytest.mark.asyncio
    async def test_set_pk_returning(self, db: AsyncConnection):
        r = await m.t1.insert(db, dict(c12=2, c13='abc'), returning=True)

        assert (r.c11, r.c12, r.c13) == (1, 2, 'abc')

        assert await m.t1.count(db) == 1
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    @pytest.mark.asyncio
    async def test_insert_by_expression(self, db: AsyncConnection):
        # TODO PostgreSQL needs cast to text but MySQL does not accept this syntax.
        r = await m.t2.insert(db, dict(c21=1, c22=Q.of("$_ + $_", 11, 22), c23=Q.of("concat($_::text, 'xyz')", 'abc')), returning=True)

        assert (r.c21, r.c22, r.c23) == (1, 33, 'abcxyz')

        assert await m.t2.count(db) == 1
        assert await m.t2.fetch(db, dict(c21=1, c22=33)) == m.t2(c21=1, c22=33, c23='abcxyz')


class TestInsertMany:
    @pytest.mark.asyncio
    async def test_insert(self, db: AsyncConnection):
        r = await m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3, c13='def'), dict(c12=4, c13='ghi')])

        assert len(r) == 3
        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

        assert await m.t1.count(db) == 3
        assert await m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

    @pytest.mark.asyncio
    async def test_insert_returning(self, db: AsyncConnection):
        await m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3, c13='def'), dict(c12=4, c13='ghi')], returning=True)

        assert await m.t1.count(db) == 3
        assert await m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

    @pytest.mark.asyncio
    async def test_insert_inconsistent_columns(self, db: AsyncConnection):
        with pytest.raises(ValueError):
            await m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3), dict(c12=4, c13='ghi')], returning=True)

    @pytest.mark.asyncio
    async def test_insert_by_expression(self, db: AsyncConnection):
        # TODO PostgreSQL needs cast to text but MySQL does not accept this syntax.
        r = await m.t1.insert_many(db, [
            dict(c12=Q.of("$_ + $_", 11, 22), c13=Q.of("concat($_::text, 'xyz')", 'abc')),
            dict(c12=Q.of("dummy()", 33, 44), c13=Q.of("", 'def')),
        ])

        assert await m.t1.count(db) == 2
        assert await m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=33, c13='abcxyz'), m.t1(c11=2, c12=77, c13='defxyz')]


class TestUpdate:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    @pytest.mark.asyncio
    async def test_update_by_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 1, dict(c12=10, c13='xyz'))

        assert r is True
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    @pytest.mark.asyncio
    async def test_update_by_pks(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t2.update(db, dict(c21=2, c22=3), dict(c23='xyz'))

        assert r is True
        assert await m.t2.fetch(db, dict(c21=2, c22=3)) == m.t2(c21=2, c22=3, c23='xyz')

    @pytest.mark.asyncio
    async def test_update_with_model(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 1, m.t1(c12=10, c13='xyz'))

        assert r is True
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    @pytest.mark.asyncio
    async def test_update_exclude_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 1, m.t1(c11=100, c12=10, c13='xyz'))

        assert r is True
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    @pytest.mark.asyncio
    async def test_update_by_expression(self, db: AsyncConnection):
        await self.prepare(db)

        # TODO PostgreSQL needs cast to text but MySQL does not accept this syntax.
        r = await m.t1.update(db, 1, dict(c12=Q.of("c12 + $_", 10), c13=Q.of("concat($_::text, c13)", "xyz")), dict(c12=lambda h: f"({h}) * 2"))

        assert r is True
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=24, c13='xyzabc')

    @pytest.mark.asyncio
    async def test_update_not_found(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 0, dict(c12=10, c13='xyz'))

        assert r is False
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    @pytest.mark.asyncio
    async def test_update_returning(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 1, dict(c12=10, c13='xyz'), returning=True)

        assert r is not None
        assert r == m.t1(c11=1, c12=10, c13='xyz')
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    @pytest.mark.asyncio
    async def test_update_returning_empty(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update(db, 0, dict(c12=10, c13='xyz'), returning=True)

        assert r is None
        assert await m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')


class TestUpdateWhere:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    @pytest.mark.asyncio
    async def test_update_where(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_exclude_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_where(db, m.t1(c11=100, c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_by_dict(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_where(db, dict(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_by_expression(self, db: AsyncConnection):
        await self.prepare(db)

        # TODO PostgreSQL needs cast to text but MySQL does not accept this syntax.
        r = await m.t1.update_where(db, dict(c12=Q.of("c12 + $_", 10), c13=Q.of("concat($_::text, c13)", 'xyz')), Q.eq(c13='def') | Q.gt(c12=3),
                              dict(c12=lambda h: f"({h}) * 2"))

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=26, c13='xyzdef'),
            m.t1(c11=3, c12=28, c13='xyzghi'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_all_ng(self, db: AsyncConnection):
        await self.prepare(db)

        with pytest.raises(ValueError):
            await m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.of(), allow_all=False)

    @pytest.mark.asyncio
    async def test_update_all_ok(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.of())

        assert r == 5
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=10, c13='xyz'),
            m.t1(c11=5, c12=10, c13='xyz'),
        ]

    @pytest.mark.asyncio
    async def test_update_returning(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3), returning=True)

        assert len(r) == 2
        assert r == [
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
        ]

        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]


class TestUpdateMany:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    @pytest.mark.asyncio
    async def test_update(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_many(db, [dict(c11=1, c12=10, c13='xyz'), dict(c11=4, c12=11, c13='uvw'), dict(c11=0, c12=0, c13='---')])

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='uvw'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_partially(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.update_many(db, [dict(c11=1, c12=10), dict(c11=4, c12=11), dict(c11=0, c12=0)])

        assert r == 2
        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='abc'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_returning(self, db: AsyncConnection):
        await self.prepare(db)

        await m.t1.update_many(db, [dict(c11=1, c12=10, c13='xyz'), dict(c11=4, c12=11, c13='uvw'), dict(c11=0, c12=0, c13='---')], returning=True)

        act = await m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='uvw'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    @pytest.mark.asyncio
    async def test_update_multi_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t2.update_many(db, [dict(c21=2, c22=3, c23='xyz'), dict(c21=0, c22=0, c23='---')])

        assert r == 1
        act = await m.t2.fetch_where(db, orders=dict(c21=True))
        assert act == [
            m.t2(c21=1, c22=2, c23='abc'),
            m.t2(c21=2, c22=3, c23='xyz'),
            m.t2(c21=3, c22=4, c23='ghi'),
        ]

    @pytest.mark.asyncio
    async def test_update_pk_missing(self, db: AsyncConnection):
        await self.prepare(db)

        with pytest.raises(ValueError):
            await m.t2.update_many(db, [dict(c21=2, c23='xyz')])

    @pytest.mark.asyncio
    async def test_update_inconsistent_columns(self, db: AsyncConnection):
        await self.prepare(db)

        with pytest.raises(ValueError):
            await m.t1.update_many(db, [dict(c11=1, c12=10), dict(c11=4, c13='xyz')])


class TestDelete:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    @pytest.mark.asyncio
    async def test_delete_by_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete(db, 2)

        assert r is True
        assert await m.t1.count(db) == 4
        assert await m.t1.fetch(db, 2) is None

    @pytest.mark.asyncio
    async def test_delete_by_pks(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t2.delete(db, dict(c21=2, c22=3))

        assert r is True
        assert await m.t2.count(db) == 2
        assert await m.t2.fetch(db, dict(c21=2, c22=3)) is None

    @pytest.mark.asyncio
    async def test_delete_returning(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete(db, 2, returning=True)

        assert r == m.t1(c11=2, c12=3, c13='def')
        assert await m.t1.count(db) == 4
        assert await m.t1.fetch(db, 2) is None

    @pytest.mark.asyncio
    async def test_delete_not_found(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete(db, 0)

        assert r is False
        assert await m.t1.count(db) == 5


class TestDeleteWhere:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    @pytest.mark.asyncio
    async def test_delete_where(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_where(db, Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [1, 4, 5]

    @pytest.mark.asyncio
    async def test_delete_returning(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_where(db, Q.eq(c13='def') | Q.gt(c12=3), returning=True)

        assert r == [m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]
        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [1, 4, 5]

    @pytest.mark.asyncio
    async def test_delete_all_ng(self, db: AsyncConnection):
        await self.prepare(db)

        with pytest.raises(ValueError):
            await m.t1.delete_where(db, Q.of(), allow_all=False)

    @pytest.mark.asyncio
    async def test_delete_all_ok(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_where(db, Q.of())

        assert r == 5
        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == []


class TestDeleteMany:
    async def prepare(self, db: AsyncConnection):
        await db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        await db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    @pytest.mark.asyncio
    async def test_delete_by_pk(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_many(db, [1, 3])

        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    @pytest.mark.asyncio
    async def test_delete_by_pks(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t2.delete_many(db, [dict(c21=1, c22=2), dict(c21=3, c22=4)])

        assert await m.t2.fetch_where(db, orders=dict(c21=True)) == [m.t2(c21=2, c22=3, c23='def')]

    @pytest.mark.asyncio
    async def test_delete_returning(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_many(db, [1, 3], returning=True)

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi')]
        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    @pytest.mark.asyncio
    async def test_delete_by_records(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_many(db, [dict(c11=1, c13='---'), dict(c11=3, c13='---')])

        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    @pytest.mark.asyncio
    async def test_delete_by_models(self, db: AsyncConnection):
        await self.prepare(db)

        r = await m.t1.delete_many(db, [m.t1(c11=1, c13='---'), m.t1(c11=3, c13='---')])

        assert [v.c11 for v in await m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]
