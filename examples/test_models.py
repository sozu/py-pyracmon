from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from uuid import UUID
from typing import Any, TYPE_CHECKING
from pyracmon import AsyncCRUDMixin, CRUDMixin, Column, Model, Table
from pyracmon.generate import ModelBase
from pyracmon.model import ForeignKey, Meta, Model, ModelColumns, Relations
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.stub import ModelTransform
from pyracmon.testing import AsyncTestingMixin, TestingMixin
from pyracmon.dialect.postgresql import AsyncPostgreSQLMixin
from pyracmon.testing.model import AsyncTestingMixin
from pyracmon.mixin_async import AsyncCRUDMixin



t1_table = Table(
    name="t1",
    columns=[
        Column(
            name="c11",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental='t1_c11_seq',
            nullable=False,
            comment="comment of c11",
        ),
        Column(
            name="c12",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=False,
            comment="comment of c12",
        ),
        Column(
            name="c13",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=False,
            comment="comment of c13",
        )],
    comment="comment of t1",
)

class t1_meta(Meta, type):
    name = "t1"
    table = t1_table
    columns = ModelColumns(t1_table.columns)

class t1(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=t1_meta):
    if TYPE_CHECKING:
        c11: int
        c12: int
        c13: str


t2_table = Table(
    name="t2",
    columns=[
        Column(
            name="c21",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="c22",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="c23",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=False,
            comment="",
        )],
    comment="",
)

class t2_meta(Meta, type):
    name = "t2"
    table = t2_table
    columns = ModelColumns(t2_table.columns)

class t2(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=t2_meta):
    if TYPE_CHECKING:
        c21: int
        c22: int
        c23: str


t3_table = Table(
    name="t3",
    columns=[
        Column(
            name="c31",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=Relations([ForeignKey('t1', 'c11')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="c32",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental='t3_c32_seq',
            nullable=False,
            comment="",
        ),
        Column(
            name="c33",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        )],
    comment="",
)

class t3_meta(Meta, type):
    name = "t3"
    table = t3_table
    columns = ModelColumns(t3_table.columns)

class t3(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=t3_meta):
    if TYPE_CHECKING:
        c31: int
        c32: int
        c33: str


t4_table = Table(
    name="t4",
    columns=[
        Column(
            name="c41",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=Relations([ForeignKey('t1', 'c11')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="c42",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=Relations([ForeignKey('t2', 'c21')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="c43",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=Relations([ForeignKey('t2', 'c22')]),
            incremental=None,
            nullable=False,
            comment="",
        )],
    comment="",
)

class t4_meta(Meta, type):
    name = "t4"
    table = t4_table
    columns = ModelColumns(t4_table.columns)

class t4(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=t4_meta):
    if TYPE_CHECKING:
        c41: int
        c42: int
        c43: int


types_table = Table(
    name="types",
    columns=[
        Column(
            name="bool_",
            ptype=bool,
            type_info=('boolean', 'bool'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="double_",
            ptype=float,
            type_info=('real', 'float4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="int_",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="string_",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="bytes_",
            ptype=bytes,
            type_info=('bytea', 'bytea'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="date_",
            ptype=date,
            type_info=('date', 'date'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="datetime_",
            ptype=datetime,
            type_info=('timestamp with time zone', 'timestamptz'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="time_",
            ptype=time,
            type_info=('time without time zone', 'time'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="delta_",
            ptype=timedelta,
            type_info=('interval', 'interval'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="uuid_",
            ptype=UUID,
            type_info=('uuid', 'uuid'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="enum_",
            ptype=object,
            type_info=('USER-DEFINED', 't_enum'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="record_",
            ptype=object,
            type_info=('USER-DEFINED', 't_record'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="array_",
            ptype=list[int],
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="deeparray_",
            ptype=list[int],
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="json_",
            ptype=object,
            type_info=('json', 'json'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="jsonb_",
            ptype=object,
            type_info=('jsonb', 'jsonb'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        )],
    comment="",
)

class types_meta(Meta, type):
    name = "types"
    table = types_table
    columns = ModelColumns(types_table.columns)

class types(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=types_meta):
    if TYPE_CHECKING:
        bool_: bool
        double_: float
        int_: int
        string_: str
        bytes_: bytes
        date_: date
        datetime_: datetime
        time_: time
        delta_: timedelta
        uuid_: UUID
        enum_: Any
        record_: Any
        array_: list[int]
        deeparray_: list[int]
        json_: Any
        jsonb_: Any


v1_table = Table(
    name="v1",
    columns=[
        Column(
            name="c11",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="comment of c11 in v1",
        ),
        Column(
            name="c12",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="c31",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="c32",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        )],
    comment="comment of v1",
)

class v1_meta(Meta, type):
    name = "v1"
    table = v1_table
    columns = ModelColumns(v1_table.columns)

class v1(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=v1_meta):
    if TYPE_CHECKING:
        c11: int
        c12: int
        c31: int
        c32: int


mv1_table = Table(
    name="mv1",
    columns=[
        Column(
            name="c11",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="comment of c11 in mv1",
        ),
        Column(
            name="c12",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="c31",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="c32",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        )],
    comment="comment of mv1",
)

class mv1_meta(Meta, type):
    name = "mv1"
    table = mv1_table
    columns = ModelColumns(mv1_table.columns)

class mv1(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=mv1_meta):
    if TYPE_CHECKING:
        c11: int
        c12: int
        c31: int
        c32: int


mv2_table = Table(
    name="mv2",
    columns=[
        Column(
            name="bool_",
            ptype=bool,
            type_info=('boolean', 'bool'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="double_",
            ptype=float,
            type_info=('real', 'float4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="int_",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="string_",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="bytes_",
            ptype=bytes,
            type_info=('bytea', 'bytea'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="date_",
            ptype=date,
            type_info=('date', 'date'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="datetime_",
            ptype=datetime,
            type_info=('timestamp with time zone', 'timestamptz'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="time_",
            ptype=time,
            type_info=('time', 'time'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="delta_",
            ptype=timedelta,
            type_info=('interval', 'interval'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="uuid_",
            ptype=UUID,
            type_info=('uuid', 'uuid'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="enum_",
            ptype=object,
            type_info=('t_enum', 't_enum'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="record_",
            ptype=object,
            type_info=('t_record', 't_record'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="array_",
            ptype=list[int],
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="deeparray_",
            ptype=list[int],
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="json_",
            ptype=object,
            type_info=('json', 'json'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        ),
        Column(
            name="jsonb_",
            ptype=object,
            type_info=('jsonb', 'jsonb'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=True,
            comment="",
        )],
    comment="",
)

class mv2_meta(Meta, type):
    name = "mv2"
    table = mv2_table
    columns = ModelColumns(mv2_table.columns)

class mv2(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=mv2_meta):
    if TYPE_CHECKING:
        bool_: bool
        double_: float
        int_: int
        string_: str
        bytes_: bytes
        date_: date
        datetime_: datetime
        time_: time
        delta_: timedelta
        uuid_: UUID
        enum_: Any
        record_: Any
        array_: list[int]
        deeparray_: list[int]
        json_: Any
        jsonb_: Any


