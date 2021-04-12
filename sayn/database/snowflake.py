from sqlalchemy import create_engine

from . import Database

db_parameters = [
    "account",
    "region",
    "user",
    "password",
    "database",
    "warehouse",
    "role",
    "schema",
]


class Snowflake(Database):
    def feature(self, feature):
        return feature in ("TABLE RENAME CHANGES SCHEMA",)

    def create_engine(self, settings):
        from snowflake.sqlalchemy import URL

        url_params = dict()
        for param in db_parameters:
            if param in settings:
                url_params[param] = settings.pop(param)

        return create_engine(URL(**url_params), **settings)

    def execute(self, script):
        conn = self.engine.connect()
        conn.connection.execute_string(script)
        conn.connection.commit()
        conn.connection.close()
