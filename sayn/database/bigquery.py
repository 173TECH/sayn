from copy import deepcopy
import csv
import datetime
import decimal
import io
from typing import List, Optional

from pydantic import validator
from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from ..core.errors import DBError
from . import Database, DDL

db_parameters = ["project", "credentials_path", "location", "dataset"]


class BigqueryDDL(DDL):
    partition: Optional[str]
    cluster: Optional[List[str]]

    @validator("cluster")
    def validate_cluster(cls, v, values):
        if len(values.get("columns")) > 0:
            missing_columns = set(v) - set([c.name for c in values.get("columns")])
            if len(missing_columns) > 0:
                raise ValueError(
                    f'Cluster contains columns not specified in the ddl: "{missing_columns}"'
                )

        return v

    def get_ddl(self):
        return {
            "columns": [c.dict() for c in self.columns],
            "indexes": {},
            "permissions": self.permissions,
            "partition": self.partition,
            "cluster": self.cluster,
            "primary_key": list(),
        }


class Bigquery(Database):
    ddl_validation_class = BigqueryDDL

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

    def _py2sqa(self, from_type, dialect=None):
        python_types = {
            int: sqltypes.Integer,
            str: sqltypes.String,
            float: sqltypes.Float,
            decimal.Decimal: sqltypes.Numeric,
            datetime.datetime: sqltypes.DateTime,
            bytes: sqltypes.LargeBinary,
            bool: sqltypes.Boolean,
            datetime.date: sqltypes.Date,
            datetime.time: sqltypes.Time,
            datetime.timedelta: sqltypes.Interval,
            list: sqltypes.ARRAY,
            dict: sqltypes.JSON,
        }

        if from_type not in python_types:
            raise ValueError(f'Type not supported "{from_type}"')
        elif dialect is not None:
            return python_types[from_type]().compile(dialect=dialect)
        else:
            return python_types[from_type]

    def _load_data_batch(self, table, data, schema):
        full_table_name = (
            f"{self.project}.{self.dataset if schema is None else schema}.{table}"
        )

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        buffer = io.BytesIO(buffer.getvalue().encode("utf-8"))

        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # autodect=True,
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
        q = f"CREATE TABLE {dst_full_table_name}"

        if ddl.get("partition") is not None:
            q += f"\nPARTITION BY {ddl['partition']}"
        if ddl.get("cluster") is not None:
            q += f"\nCLUSTER BY {', '.join(ddl['cluster'])}"

        q += (
            f" AS (SELECT * from {src_full_table_name});\n"
            f"DROP TABLE {src_full_table_name}"
        )

        if execute:
            self.execute(q)

        return q

    def _create_table_select(
        self, table, schema, select, view=False, ddl=dict(), execute=False
    ):
        """Returns SQL code for a create table from a select statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): A SQL SELECT query to build the table with
            view (bool): Indicates if the object to create is a view. Defaults to creating a table
            ddl (dict): Optionally specify a ddl dict. If provided, a `CREATE` with column specification
                followed by an `INSERT` rather than a `CREATE ... AS SELECT ...` will be issued
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the CREATE...AS
        """
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        q = f"CREATE {table_or_view}{if_not_exists} {table}\n"

        if ddl.get("partition") is not None:
            q += f"\nPARTITION BY {ddl['partition']}"
        if ddl.get("cluster") is not None:
            q += f"\nCLUSTER BY {', '.join(ddl['cluster'])}"
        q += f"\nAS (\n{select}\n);"

        if execute:
            self.execute(q)

        return q

    def _create_table_ddl(self, table, schema, ddl, execute=False):
        """Returns SQL code for a create table from a select statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the CREATE TABLE statement
        """
        if len(ddl["columns"]) == 0:
            raise DBError(
                self.name, self.db_type, "DDL is missing columns specification"
            )
        table_name = table
        table = f"{schema+'.' if schema else ''}{table_name}"

        # List of reserved keywords so columns are quoted
        # TODO find a better way
        reserved = ("from", "to", "primary")
        columns = [
            {k: f'"{v}"' if k == "name" and v in reserved else v for k, v in c.items()}
            for c in ddl["columns"]
        ]

        columns = "\n    , ".join(
            [
                (
                    f'{c["name"]} {c["type"]}'
                    f'{" NOT NULL" if c.get("not_null", False) else ""}'
                )
                for c in columns
            ]
        )

        q = ""
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        q += f"CREATE TABLE{if_not_exists} {table} (\n      {columns}\n)"

        if ddl.get("partition") is not None:
            q += f"\nPARTITION BY {ddl['partition']}"
        if ddl.get("cluster") is not None:
            q += f"\nCLUSTER BY {', '.join(ddl['cluster'])}"
        q += ";"

        if execute:
            self.execute(q)

        return q
