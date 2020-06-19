import csv
import io
from sqlalchemy import create_engine

from .database import Database

db_parameters = ["host", "user", "password", "port", "dbname"]


class Postgresql(Database):
    sql_features = ["DROP CASCADE"]

    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()
        for param in db_parameters:
            if param in settings:
                settings["connect_args"][param] = settings.pop(param)

        engine = create_engine("postgresql://", **settings)
        self.setup_db(name, name_in_settings, db_type, engine)

    def select_stream(self, query, params=None):
        with self.engine.connect().execution_options(stream_results=True) as connection:
            if params is not None:
                res = connection.execute(query, **params)
            else:
                res = connection.execute(query)

            for record in res.fetchall():
                yield dict(zip(res.keys(), record))

    def load_data_stream(self, table, schema, data_iter):
        full_table_name = f"{'' if schema is None else schema + '.'}{table}"
        connection = self.engine.connect().connection
        with connection.cursor() as cursor:
            buffer = None
            writer = None
            has_rows = False
            for i, record in enumerate(data_iter):
                if i % 100000 == 0:
                    if has_rows:
                        buffer.seek(0)
                        cursor.copy_from(buffer, full_table_name, null="")
                        connection.commit()
                    buffer = io.StringIO()
                    writer = csv.DictWriter(
                        buffer, fieldnames=record.keys(), delimiter="\t"
                    )
                    has_rows = False

                writer.writerow(record)
                has_rows = True

            if has_rows:
                buffer.seek(0)
                cursor.copy_from(buffer, full_table_name, null="")
                connection.commit()
