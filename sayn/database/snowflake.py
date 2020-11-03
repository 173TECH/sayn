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

    def move_table(
        self, src_table, src_schema, dst_table, dst_schema, ddl, execute=False
    ):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_schema+'.' if dst_schema else ''}{dst_table};"
        q = "\n".join((drop, rename))

        if execute:
            self.execute(q)

        return q
