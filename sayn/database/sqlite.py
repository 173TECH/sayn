from sqlalchemy import create_engine

from ..core.errors import Err, Ok
from . import Database

db_parameters = ["database"]


class Sqlite(Database):
    sql_features = [
        "CREATE TABLE NO PARENTHESES",
        "INSERT TABLE NO PARENTHESES",
        "NO SET SCHEMA",
    ]

    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

        database = settings.pop("database")

        engine = create_engine(f"sqlite:///{database}", **settings)
        self.setup_db(name, name_in_settings, db_type, engine)

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
                    db=self.name,
                    script=script,
                )
            finally:
                return result
