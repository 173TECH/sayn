from sqlalchemy import create_engine, event
from itertools import groupby

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

    def _list_databases(self):
        return [""]

    def _get_table_type(self, type):
        if type == "table":
            return "table"
        elif type == "view":
            return "view"

    def _list_objects(self, databases):
        """List the accessible databases for this connection."""
        objects = list()
        queries = ""
        for database in databases:
            if database == "":

                queries += """SELECT "" AS table_schema
                                    , "" AS table_catalog
                                    , name AS table_name
                                    , type AS table_type
                                 FROM (SELECT * FROM sqlite_schema UNION ALL
                                 SELECT * FROM sqlite_temp_schema)
                            """
            queries += "\nUNION ALL\n"

        queries = "\n".join(queries.split("\n")[:-2])
        objects = {
            db.lower(): {
                schema.lower(): {
                    table.lower(): {
                        "type": self._get_table_type(type["table_type"]) for type in ggg
                    }
                    for table, ggg in groupby(gg, lambda x: x["table_name"])
                }
                for schema, gg in groupby(g, lambda x: x["table_schema"])
            }
            for db, g in groupby(
                sorted(
                    self.read_data(queries),
                    key=lambda x: (
                        x["table_catalog"],
                        x["table_schema"],
                        x["table_name"],
                    ),
                ),
                key=lambda x: x["table_catalog"],
            )
        }

        return objects

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.connection.executescript(script)

    def _obj_str(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        if schema is not None:
            raise ValueError("Sqlite doesn't support schemas")

        if database is not None:
            raise ValueError("Sqlite doesn't support databases")

        return (
            f"{database + '.' if database is not None else ''}"
            f"{schema + '.' if schema is not None else ''}"
            f"{table}"
        )

    def _fully_qualify(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        return self._obj_str(database, schema, table)
