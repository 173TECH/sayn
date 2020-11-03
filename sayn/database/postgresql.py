import csv
import io
from sqlalchemy import create_engine

from . import Database

db_parameters = ["host", "user", "password", "port", "dbname"]


class Postgresql(Database):
    sql_features = ["DROP CASCADE"]

    def create_engine(self, settings):
        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()
        for param in db_parameters:
            if param in settings:
                settings["connect_args"][param] = settings.pop(param)

        return create_engine("postgresql://", **settings)

    def _load_data_batch(self, table, data, schema):
        full_table_name = f"{'' if schema is None else schema + '.'}{table}"
        copy_sql = f"COPY {full_table_name} FROM STDIN " "CSV DELIMITER ',' QUOTE '\"'"

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=data[0].keys())
        writer.writerows(data)
        buffer.seek(0)
        connection = self.engine.connect().connection
        with connection.cursor() as cursor:
            cursor.copy_expert(copy_sql, buffer)
            connection.commit()
