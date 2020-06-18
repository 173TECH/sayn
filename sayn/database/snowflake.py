import logging

from sqlalchemy import create_engine

from .database import Database

db_parameters = [
    "account",
    "region",
    "user",
    "password",
    "database",
    "warehouse",
    "role",
    "schema",
    "host",
    "port",
]


class Snowflake(Database):
    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

        from snowflake.sqlalchemy import URL

        for logger_name in ["snowflake.connector", "botocore", "boto3"]:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

        url_params = dict()
        for param in db_parameters:
            if param in settings:
                url_params[param] = settings.pop(param)

        engine = create_engine(URL(**url_params), **settings)

        self.setup_db(name, name_in_settings, db_type, engine)

    def execute(self, script):
        conn = self.engine.connect()
        conn.connection.execute_string(script)
        conn.connection.commit()
        conn.connection.close()

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_schema+'.' if dst_schema else ''}{dst_table};"

        return "\n".join((drop, rename))
