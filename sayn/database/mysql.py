import datetime
import decimal

from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from . import Database

db_parameters = ["host", "user", "password", "port", "database"]


class Mysql(Database):
    def feature(self, feature):
        return feature in (
            "CAN REPLACE VIEW",
            "CANNOT SPECIFY DDL IN SELECT",
            "TABLE RENAME CHANGES SCHEMA",
        )

    def create_engine(self, settings):
        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()
        for param in db_parameters:
            if param in settings:
                if param == "port":
                    value = int(settings.pop(param))
                else:
                    value = settings.pop(param)
                settings["connect_args"][param] = value

        return create_engine("mysql+pymysql://", **settings)

    def execute(self, script):
        for s in script.split(";"):
            if len(s.strip()) > 0:
                self.engine.execute(s)

    def _py2sqa(self, from_type):
        python_types = {
            int: sqltypes.BigInteger,
            str: sqltypes.Text,
            float: sqltypes.Float,
            decimal.Decimal: sqltypes.Numeric,
            datetime.datetime: sqltypes.TIMESTAMP,
            bytes: sqltypes.LargeBinary,
            bool: sqltypes.Boolean,
            datetime.date: sqltypes.Date,
            datetime.time: sqltypes.Time,
            datetime.timedelta: sqltypes.Interval,
            list: sqltypes.ARRAY,
            dict: sqltypes.JSON,
        }

        if from_type not in python_types:
            raise ValueError(f'Type not supported "{from_type}"')
        else:
            return python_types[from_type]().compile(dialect=self.engine.dialect)
