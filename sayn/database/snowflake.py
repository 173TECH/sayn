import logging

from sqlalchemy import create_engine, exc

from ..core.errors import Err, Ok
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
        try:
            conn = self.engine.connect()
            conn.connection.execute_string(script)
            conn.connection.commit()
            conn.connection.close()
            result = Ok()
        except exc.ProgrammingError as e:
            result = Err(
                "database_error",
                "sql_execution_error",
                message="\n ".join([s.strip() for s in e.args]),
                exception=e,
                db=self.name,
                script=script,
            )
        except Exception as e:
            result = Err(
                "database_error",
                "sql_execution_error",
                exception=e,
                db=self.name,
                script=script,
            )
        finally:
            return result

    def move_table(
        self, src_table, src_schema, dst_table, dst_schema, ddl, execute=False
    ):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_schema+'.' if dst_schema else ''}{dst_table};"
        q = "\n".join((drop, rename))

        if execute:
            result = self.execute(q)
            if result.is_err:
                return result

        return Ok(q)
