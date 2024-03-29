from typing import Any, Optional
from pyracmon import Model, CRUDMixin
from pyracmon.model_graph import GraphEntityMixin
from pyracmon.stub import ModelTransform
from pyracmon.testing import TestingMixin
from pyracmon.dialect.postgresql import PostgreSQLMixin

class blog(PostgreSQLMixin, CRUDMixin, GraphEntityMixin, ModelTransform):
    id: int = ...
    title: str = ...

class blog_category(PostgreSQLMixin, CRUDMixin, GraphEntityMixin, ModelTransform):
    id: int = ...
    blog_id: int = ...
    name: str = ...

class image(PostgreSQLMixin, CRUDMixin, GraphEntityMixin, ModelTransform):
    post_id: int = ...
    url: str = ...

class post(PostgreSQLMixin, CRUDMixin, GraphEntityMixin, ModelTransform):
    id: int = ...
    blog_id: int = ...
    title: str = ...
    content: str = ...

class post_comment(PostgreSQLMixin, CRUDMixin, GraphEntityMixin, ModelTransform):
    id: int = ...
    post_id: int = ...
    likes: int = ...
    content: str = ...
