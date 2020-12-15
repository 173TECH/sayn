from copy import deepcopy
import csv
import io

from google.cloud import bigquery
from sqlalchemy import create_engine

from . import Database

db_parameters = ["project", "credentials_path", "location", "dataset"]


class Bigquery(Database):
    sql_features = []
    project = None
    dataset = None

    def create_engine(self, settings):
        settings = deepcopy(settings)
        self.project = settings.pop("project")

        url = f"bigquery://{self.project}"
        if "dataset" in settings:
            self.dataset = settings.pop("dataset")
            url += "/" + self.dataset

        return create_engine(url, **settings)

    def _load_data_batch(self, table, data, schema):
        full_table_name = (
            f"{self.project}.{self.dataset if schema is None else schema}.{table}"
        )

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=data[0].keys())
        writer.writerows(data)
        buffer.seek(0)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
        )

        client = self.engine.raw_connection()._client
        job = client.load_table_from_file(
            buffer, full_table_name, job_config=job_config
        )
        job.result()

    def _move_table(
        self, src_table, src_schema, dst_table, dst_schema, ddl, execute=False
    ):
        """Returns SQL code to rename a table and change schema.

        Note:
            Table movement is performed as a series of ALTER statements:

              * CREATE TABLE dst_table AS (SELECT * FROM src_table)
              * DROP src_tabe

        Args:
            src_table (str): The source table name
            src_schema (str): The source schema or None
            dst_table (str): The target table name
            dst_schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for moving the table
        """
        src_full_table_name = f"{src_schema+'.' if src_schema else ''}{src_table}"
        dst_full_table_name = f"{dst_schema+'.' if dst_schema else ''}{dst_table}"
        q = (
            f"CREATE TABLE {dst_full_table_name} AS (SELECT * from {src_full_table_name});"
            f"DROP TABLE {src_full_table_name}"
        )

        if execute:
            self.execute(q)

        return q
