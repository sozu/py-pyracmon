from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID, uuid1, uuid3
from .util import Matcher, config


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


class TestingMixin:
    """
    Mixin class for model types providing methods designed for testing.
    """
    @classmethod
    def by(cls, index):
        """
        Set current fixture index.

        Parameters
        ----------
        index: int
            Fixture index.

        Returns
        -------
        type[TestingMixin]
            This type.
        """
        TestingState.set_index(cls, index)
        return cls

    @classmethod
    def fixture(cls, db, variable=None, index=None):
        """
        Insert record with auto-generated column values.

        Parameters
        ----------
        db: Connection
            DB connection. If a value evaluated to be `False` in boolean context, generated model is not inserted and just returned.
        variable: int | dict | model | [dict] | [model]
            When `int`, inserts records as many as the number. All of their column values are generated.
            When `dict`, model object or a list of them, inserts record(s) represented by them. Unspecified column values are generated.
        index: int
            Use this to specify index used to generate column values explicitly. If set, indexing state is not updated.

        Returns
        -------
        model | [model]
            Inserted model(s).
        """
        if isinstance(variable, int):
            num = variable
            index = TestingState.inc(cls, num) if index is None else index
            models = [_generate_model(cls, index+i) for i in range(num)]
            if db:
                cls.inserts(db, models)
            return models
        elif isinstance(variable, (cls, dict)):
            num = 1
            index = TestingState.inc(cls, num) if index is None else index
            model = _generate_model(cls, index, variable)
            if db:
                cls.insert(db, _generate_model(cls, index, variable))
            return model
        elif isinstance(variable, list):
            num = len(variable)
            index = TestingState.inc(cls, num) if index is None else index
            models = [_generate_model(cls, index+i, v) for i, v in enumerate(variable)]
            if db:
                cls.inserts(db, models)
            return models
        else:
            raise ValueError(f"Second argument of fixture() must be an int, dict, model or list of dict or model but {type(variable)} is passed.")

    def match(self, **expected):
        """
        Tests columns values matches to expected values.

        Parameters
        ----------
        expected: {str: (object | Matcher)}
            Expected values.

        Returns
        -------
        bool
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


def _generate_model(model, index, model_or_dict=None):
    values = {}

    if isinstance(model_or_dict, TestingMixin):
        values = {c.name:v for c, v in model_or_dict}
    elif isinstance(model_or_dict, dict):
        values = dict(**model_or_dict)

    values.update(**{c.name:_generate_value(model.table, c, index) for c in model.columns if c.name not in values and not c.pk})

    return model(**values)


fixed_uuid=uuid1(0, 0)


def _generate_value(table, column, index):
    """
    Generates a value for the column on an index.

    Parameters
    ----------
    table: Table
        Table.
    column: Column
        Column.
    index: int
        Index

    Returns
    -------
    object
        Generated value.
    """
    mapping = config().fixture_mapping
    if mapping:
        value = mapping(table, column, index)
        if value is not None:
            return value

    tz_aware = config().fixture_tz_aware

    if column.ptype is bool:
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
    else:
        return None
