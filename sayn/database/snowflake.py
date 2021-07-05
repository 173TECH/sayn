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
        return feature in ("TABLE RENAME CHANGES SCHEMA",)

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

    def _load_data_batch(self, table, data, schema):
        """Implements the load of a single data batch for `load_data`.

        Defaults to an insert many statement, but it's overloaded for specific
        database connector for more efficient methods.

        Args:
            table (str): The name of the target table
            data (list): A list of dictionaries to load
            schema (str): An optional schema to reference the table
        """
        full_table_name = f"{'' if schema is None else schema + '.'}{table}"
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
