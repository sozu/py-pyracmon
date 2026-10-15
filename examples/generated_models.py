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



blog_table = Table(
    name="blog",
    columns=[
        Column(
            name="id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental='blog_id_seq',
            nullable=False,
            comment="",
        ),
        Column(
            name="title",
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

class blog_meta(Meta, type):
    name = "blog"
    table = blog_table
    columns = ModelColumns(blog_table.columns)

class blog(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=blog_meta):
    if TYPE_CHECKING:
        id: int
        title: str


blog_category_table = Table(
    name="blog_category",
    columns=[
        Column(
            name="id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental='blog_category_id_seq',
            nullable=False,
            comment="",
        ),
        Column(
            name="blog_id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=Relations([ForeignKey('blog', 'id')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="name",
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

class blog_category_meta(Meta, type):
    name = "blog_category"
    table = blog_category_table
    columns = ModelColumns(blog_category_table.columns)

class blog_category(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=blog_category_meta):
    if TYPE_CHECKING:
        id: int
        blog_id: int
        name: str


image_table = Table(
    name="image",
    columns=[
        Column(
            name="post_id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=Relations([ForeignKey('post', 'id')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="url",
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

class image_meta(Meta, type):
    name = "image"
    table = image_table
    columns = ModelColumns(image_table.columns)

class image(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=image_meta):
    if TYPE_CHECKING:
        post_id: int
        url: str


post_table = Table(
    name="post",
    columns=[
        Column(
            name="id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental='post_id_seq',
            nullable=False,
            comment="",
        ),
        Column(
            name="blog_id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=Relations([ForeignKey('blog', 'id')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="title",
            ptype=str,
            type_info=('text', 'text'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="content",
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

class post_meta(Meta, type):
    name = "post"
    table = post_table
    columns = ModelColumns(post_table.columns)

class post(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=post_meta):
    if TYPE_CHECKING:
        id: int
        blog_id: int
        title: str
        content: str


post_comment_table = Table(
    name="post_comment",
    columns=[
        Column(
            name="id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=True,
            fk=None,
            incremental='post_comment_id_seq',
            nullable=False,
            comment="",
        ),
        Column(
            name="post_id",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=Relations([ForeignKey('post', 'id')]),
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="likes",
            ptype=int,
            type_info=('integer', 'int4'),
            pk=False,
            fk=None,
            incremental=None,
            nullable=False,
            comment="",
        ),
        Column(
            name="content",
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

class post_comment_meta(Meta, type):
    name = "post_comment"
    table = post_comment_table
    columns = ModelColumns(post_comment_table.columns)

class post_comment(ModelBase, AsyncPostgreSQLMixin, AsyncTestingMixin, AsyncCRUDMixin, GraphEntityMixin, Model, metaclass=post_comment_meta):
    if TYPE_CHECKING:
        id: int
        post_id: int
        likes: int
        content: str


