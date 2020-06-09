import logging

from sqlalchemy import create_engine

from .database import Database


class Snowflake(Database):
    def __init__(self, name, name_in_settings, settings):
        self.dialect = "snowflake"
        self.db_type = settings.pop("type")

        self.name = name
        self.name_in_settings = name_in_settings

        from snowflake.sqlalchemy import URL

        for logger_name in ["snowflake.connector", "botocore", "boto3"]:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

        self.engine = create_engine(URL(**settings["connect_args"]))

        self.create_metadata()

    def execute(self, script):
        conn = self.engine.connect()
        conn.connection.execute_string(script)
        conn.connection.close()

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_schema+'.' if dst_schema else ''}{dst_table};"

        return "\n".join((drop, rename))
