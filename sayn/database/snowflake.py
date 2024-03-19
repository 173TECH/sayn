import csv
from pathlib import Path
import tempfile

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
    def feature(self, feature):
        return feature in (
            "TABLE RENAME CHANGES SCHEMA",
            "CAN REPLACE TABLE",
        )

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

    def _list_databases(self):
        """List the accessible databases for this connection."""
        databases = self.read_data("SHOW DATABASES;")
        databases = [db["name"].lower() for db in databases] + [""]
        return databases

    def _get_table_type(self, type):
        if type == "BASE TABLE":
            return "table"
        elif type == "VIEW":
            return "view"

    def _load_data_batch(self, table, data, schema, db):
        """Implements the load of a single data batch for `load_data`.

        Defaults to an insert many statement, but it's overloaded for specific
        database connector for more efficient methods.

        Args:
            table (str): The name of the target table
            data (list): A list of dictionaries to load
            schema (str): An optional schema to reference the table
        """
        full_table_name = f"{'' if db is None else db + '.'}{'' if schema is None else schema + '.'}{table}"
        template = self._jinja_env.get_template("snowflake_load_batch.sql")
        fname = "batch.csv"

        with tempfile.TemporaryDirectory() as tmpdirname:
            with (Path(tmpdirname) / fname).open("w") as f:
                writer = csv.DictWriter(
                    f, fieldnames=data[0].keys(), delimiter="\t", escapechar="\\"
                )
                writer.writeheader()
                writer.writerows(data)

            self.execute(
                template.render(
                    full_table_name=full_table_name,
                    temp_file_directory=tmpdirname,
                    temp_file_name=fname,
                )
            )

    def move_table(
        self,
        src_table,
        dst_table,
        src_schema=None,
        dst_schema=None,
        src_db=None,
        dst_db=None,
        **ddl,
    ):
        full_src_table = (
            f"{src_db + '.' if src_db is not None else ''}"
            f"{src_schema + '.' if src_schema is not None else ''}"
            f"{src_table}"
        )
        select = f"SELECT * FROM {full_src_table}"
        create_or_replace = self.create_table(
            dst_table, dst_schema, dst_db, select=select, replace=True, **ddl
        )

        return "\n\n".join((create_or_replace, f"DROP TABLE {full_src_table}"))

    def create_table(
        self,
        table,
        schema=None,
        db=None,
        select=None,
        replace=False,
        **ddl,
    ):
        db_name = db or ""
        schema_name = schema or ""
        full_name = fully_qualify(table, schema, db)
        if (
            db_name in self._requested_objects
            and schema_name in self._requested_objects[db_name]
            and table in self._requested_objects[db_name][schema_name]
        ):
            db_info = self._requested_objects[db_name][schema_name][table]
            object_type = db_info.get("type")
            table_exists = bool(object_type == "table")
            view_exists = bool(object_type == "view")
        else:
            db_info = dict()
            table_exists = True
            view_exists = True

        template = self._jinja_env.get_template("create_table.sql")

        return template.render(
            table_name=table,
            full_name=full_name,
            view_exists=view_exists,
            table_exists=table_exists,
            select=select,
            replace=True,
            can_replace_table=self.feature("CAN REPLACE TABLE"),
            needs_cascade=self.feature("NEEDS CASCADE"),
            cannot_specify_ddl_select=self.feature("CANNOT SPECIFY DDL IN SELECT"),
            all_columns_have_type=len(
                [c for c in ddl.get("columns", dict()) if c.get("type") is not None]
            ),
            **ddl,
        )


def fully_qualify(name, schema=None, db=None):
    return f"{db+'.' if db is not None else ''}{schema+'.' if schema is not None else ''}{name}"
