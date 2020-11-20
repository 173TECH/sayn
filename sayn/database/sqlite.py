from sqlalchemy import create_engine, event
from pydantic import validator

from . import Database, DDL

db_parameters = ["database"]


class SQLiteDDL(DDL):
    @validator("columns")
    def pk_in_columns(cls, v, values):
        for c in v:
            if c.primary:
                raise ValueError(
                    "Primary key not currently supported for SQLite in SAYN."
                )
        return v

    @validator("indexes")
    def pk_in_indexes(cls, v, values):
        if "primary_key" in v.keys():
            raise ValueError("Primary key not currently supported for SQLite in SAYN.")
        return v


class Sqlite(Database):
    ddl_validation_class = SQLiteDDL

    sql_features = [
        "CREATE TABLE NO PARENTHESES",
        "INSERT TABLE NO PARENTHESES",
        "NO SET SCHEMA",
        # "PRIMARY KEY CREATE DDL ONLY",
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
            connection.connection.executescript(script)
