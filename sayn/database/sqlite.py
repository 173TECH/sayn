from sqlalchemy import create_engine, event
from pydantic import validator

from . import Database, DDL

db_parameters = ["database"]


class SQLiteDDL(DDL):
    @validator("indexes")
    # for SQLite, primary_key is only allowed in indexes if defined as well in columns as SQLite does not support ALTER for primary keys
    def pk_in_indexes(cls, v, values):
        if "primary_key" in v.keys():
            col_pk = False
            for c in values.get("columns", []):
                if c.primary:
                    col_pk = True
            if not col_pk:
                raise ValueError(
                    "Setting a primary key for SQLite in SAYN requires to use the columns definition in the ddl entry."
                )
        return v


class Sqlite(Database):
    ddl_validation_class = SQLiteDDL

    sql_features = [
        "CREATE TABLE NO PARENTHESES",
        "INSERT TABLE NO PARENTHESES",
        "NO SET SCHEMA",
        "NO ALTER INDEXES",
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
