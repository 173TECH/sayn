import datetime
import decimal

from sqlalchemy.sql import sqltypes

python_types = {
    int: sqltypes.BigInteger,
    str: sqltypes.Unicode,
    float: sqltypes.Float,
    decimal.Decimal: sqltypes.Numeric,
    datetime.datetime: sqltypes.DateTime,
    bytes: sqltypes.LargeBinary,
    bool: sqltypes.Boolean,
    datetime.date: sqltypes.Date,
    datetime.time: sqltypes.Time,
    datetime.timedelta: sqltypes.Interval,
    list: sqltypes.ARRAY,
    dict: sqltypes.JSON,
}


def py2sqa(from_type, dialect=None):
    if from_type not in python_types:
        raise ValueError(f'Type not supported "{from_type}"')
    elif dialect is not None:
        return python_types[from_type]().compile(dialect=dialect)
    else:
        return python_types[from_type]
