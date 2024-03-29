from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid1, uuid3
from typing import Optional, Union, TypeVar, Any, overload, TYPE_CHECKING
from typing_extensions import Self
from pyracmon.config import PyracmonConfiguration
from pyracmon.connection import Connection
from pyracmon.mixin import CRUDMixin
from pyracmon.model import Model
from pyracmon.graph.typing import issubgeneric
from pyracmon.dialect.shared import MultiInsertMixin, TruncateMixin
from .util import default_test_config, Matcher


if TYPE_CHECKING:
    class TestingModel(MultiInsertMixin, CRUDMixin):
        pass
else:
    class TestingModel():
        pass


M = TypeVar('M', bound=TestingModel)


class TestingState:
    indexes = {}

    @classmethod
    def reset(cls, model=None):
        cls.indexes.clear()

    @classmethod
    def inc(cls, model, count):
        v = cls.indexes.setdefault(model, 1)
        cls.indexes[model] = v + count
        return v

    @classmethod
    def set_index(cls, model, index):
        cls.indexes[model] = index


class TestingMixin(TestingModel):
    """
    Mixin class for model types providing methods designed for testing.
    """
    @classmethod
    def by(cls, index: int) -> Self:
        """
        Set current fixture index.

        Args:
            index: Fixture index.
        Returns:
            This type.
        """
        TestingState.set_index(cls, index)
        return cls # type: ignore

    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: None = None,
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> list[Self]: ...
    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: int,
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> list[Self]: ...
    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: Self,
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> Self: ...
    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: dict[str, Any],
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> Self: ...
    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: list[Self],
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> list[Self]: ...
    @overload
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: list[dict[str, Any]],
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> list[Self]: ...
    @classmethod
    def fixture(
        cls,
        db: Optional[Connection],
        variable: Optional[Union[int, dict[str, Any], Self, list[dict[str, Any]], list[Self]]] = None,
        index: Optional[int] = None,
        cfg: Optional[PyracmonConfiguration] = None,
    ) -> Union[Self, list[Self]]:
        """
        Inserts record(s) with auto-generated column values.

        Args:
            db: DB connection. If a value evaluated to be `False` in boolean context, generated model is not inserted and just returned.
            variable: When `int`, inserts records as many as the number. All of their column values are generated.
                When `dict`, model object or a list of them, inserts record(s) represented by them. Unspecified column values are generated.
            index: Use this to specify index used to generate column values explicitly. If set, indexing state is not updated.
            cfg: Configuration used to control the generation of fixuture values.
                This argument is prepared only for internal use and can be changed or removed in future version.
        Returns:
            Inserted model(s).
        """
        if variable is None or isinstance(variable, int):
            num = variable or 1
            index = TestingState.inc(cls, num) if index is None else index
            models = [_generate_model(cls, index+i, None, cfg) for i in range(num)]
            if db:
                cls.inserts(db, models)
            return models
        elif isinstance(variable, (cls, dict)):
            num = 1
            index = TestingState.inc(cls, num) if index is None else index
            model = _generate_model(cls, index, variable, cfg)
            if db:
                cls.insert(db, model)
            return model
        elif isinstance(variable, list):
            num = len(variable)
            index = TestingState.inc(cls, num) if index is None else index
            models = [_generate_model(cls, index+i, v, cfg) for i, v in enumerate(variable)]
            if db:
                cls.inserts(db, models)
            return models
        else:
            raise ValueError(f"Second argument of fixture() must be an int, dict, model or list of dict or model but {type(variable)} is passed.")

    def match(self, **expected: Union[Matcher, Any]) -> bool:
        """
        Tests columns values matches to expected values.

        .. warning::
            This method will be replaced in different implementation.

        Args:
            expected: Expected values.
        Returns
            Matches or not.
        """
        for k, v in expected.items():
            actual = getattr(self, k)

            if isinstance(v, Matcher):
                if not v.match(actual):
                    return False
            else:
                if v != actual:
                    return False
        return True


def truncate(db: Connection, *models: type[TruncateMixin]):
    """
    Truncate tables in order.

    Args:
        db: DB connection.
        tables: Models of tables to truncate.
    """
    if len(models) == 0:
        raise ValueError(f"No tables are specified. Did you forget to pass DB connection at the first argument?")

    for m in models:
        m.truncate(db)


def _generate_model(model: type[M], index, model_or_dict, cfg) -> M:
    values = {}

    if isinstance(model_or_dict, TestingMixin):
        values = {c.name:v for c, v in model_or_dict}
    elif isinstance(model_or_dict, dict):
        values = dict(**model_or_dict)

    values.update(**{c.name:_generate_value(model.table, c, index, cfg) for c in model.columns if c.name not in values and not c.pk})

    return model(**values)


fixed_uuid=uuid1(0, 0)


def _generate_value(table, column, index, cfg):
    """
    Generates a value for the column on an index.

    Args:
        table: Table.
        column: Column.
        index: Index
    Returns:
        Generated value.
    """
    cfg = cfg or default_test_config()

    mapping = cfg.fixture_mapping
    if mapping:
        value = mapping(table, column, index)
        if value is not None:
            return value

    tz_aware = cfg.fixture_tz_aware

    if column.nullable and cfg.fixture_ignore_nullable:
        return None
    elif column.fk and cfg.fixture_ignore_fk:
        return None
    elif column.ptype is bool:
        return True
    elif column.ptype is float:
        return float(f"{index}.{index+1}")
    elif column.ptype is int:
        return index
    elif column.ptype is Decimal:
        return Decimal(index)
    elif column.ptype is str:
        return f"{column.name}-{index}"
    elif column.ptype is bytes:
        return f"{column.name}-{index}".encode()
    elif column.ptype is date:
        return date.today()
    elif column.ptype is datetime:
        return datetime.now().astimezone() if tz_aware else datetime.now()
    elif column.ptype is time:
        return datetime.now().astimezone().time() if tz_aware else datetime.now().time()
    elif column.ptype is timedelta:
        return timedelta(days=index+1)
    elif column.ptype is UUID:
        return str(uuid3(fixed_uuid, f"{table.name}-{column.name}-{index}"))
    elif issubgeneric(column.ptype, list):
        return []
    elif isinstance(column.ptype, type) and issubclass(column.ptype, Enum):
        return next(iter(column.ptype), None)
    else:
        return None
