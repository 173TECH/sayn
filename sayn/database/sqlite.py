from sqlalchemy import create_engine, event

from typing import Optional

from . import Database

db_parameters = ["database"]


class Sqlite(Database):
    def feature(self, feature):
        return feature in (
            "CANNOT ALTER INDEXES",
            "CANNOT SET SCHEMA",
            "CANNOT SPECIFY DDL IN SELECT",
            "NO SCHEMA SUPPORT",
        )

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

    def _obj_str(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        if schema is not None:
            print(schema)
            raise ValueError("Sqlite doesn't support schemas")

        return (
            f"{database + '.' if database is not None else ''}"
            f"{schema + '.' if schema is not None else ''}"
            f"{table}"
        )

    def _fully_qualify(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        return self._obj_str(database, schema, table)
