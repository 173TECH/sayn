from sqlalchemy import create_engine, event

from ..core.errors import Err, Ok
from . import Database

db_parameters = ["database"]


class Sqlite(Database):
    sql_features = [
        "CREATE TABLE NO PARENTHESES",
        "INSERT TABLE NO PARENTHESES",
        "NO SET SCHEMA",
    ]

    def create_engine(self, settings):
        database = settings.pop("database")

        engine = create_engine(f"sqlite:///{database}", **settings)

        # this is set to fix a SQLite setting which can prevent a second execution of SAYN.
        # More info on this command here: https://sqlite.org/pragma.html#pragma_legacy_alter_table
        @event.listens_for(engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA legacy_alter_table = ON")
            cursor.close()

        return engine

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            try:
                connection.connection.executescript(script)
                result = Ok()
            except Exception as e:
                result = Err(
                    "database_error",
                    "sql_execution_error",
                    exception=e,
                    message=f"{e}",
                    db=self.name,
                    script=script,
                )
            finally:
                return result
