from copy import deepcopy
from collections import Counter
import csv
import datetime
import decimal
from itertools import groupby
import io
from typing import List, Optional, Union
from uuid import UUID

import orjson
from pydantic import validator, Extra, BaseModel
from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from . import Database, Columns, Hook, BaseDDL

from ..core.errors import DBError, Ok

db_parameters = ["project", "credentials_path", "location", "dataset"]


class DDL(BaseDDL):
    class Properties(BaseModel):
        partition: Optional[str]
        cluster: Optional[List[str]]

        class Config:
            extra = Extra.forbid

    columns: List[Union[str, Columns]] = list()
    properties: Optional[Properties]
    post_hook: List[Hook] = list()

    class Config:
        extra = Extra.forbid

    @validator("columns", pre=True)
    def transform_str_cols(cls, v):
        if v is not None and isinstance(v, List):
            return [{"name": c} if isinstance(c, str) else c for c in v]
        else:
            return v

    @validator("columns")
    def columns_unique(cls, v):
        dupes = {k for k, v in Counter([e.name for e in v]).items() if v > 1}
        if len(dupes) > 0:
            raise ValueError(f"Duplicate columns: {','.join(dupes)}")
        else:
            return v

    @validator("properties")
    def validate_properties(cls, v, values):
        if v is not None and v.cluster is not None:
            if len(values.get("columns")) > 0:
                missing_columns = set(v) - set([c.name for c in values.get("columns")])
                if len(missing_columns) > 0:
                    raise ValueError(
                        f'Cluster contains columns not specified in the ddl: "{missing_columns}"'
                    )

        return v

    def get_ddl(self):
        result = self.base_ddl()
        properties = list()
        if self.properties is not None:
            if self.properties.cluster is not None:
                properties.append({"cluster": self.properties.cluster})
                result["cluster"] = self.properties.cluster

            if self.properties.partition is not None:
                properties.append({"partition": self.properties.partition})
                result["partition"] = self.properties.partition

        return result


class Bigquery(Database):
    DDL = DDL

    project = None
    dataset = None

    def feature(self, feature):
        return feature in (
            "CAN REPLACE TABLE",
            "CAN REPLACE VIEW",
            "CANNOT CHANGE SCHEMA",
        )

    def create_engine(self, settings):
        settings = deepcopy(settings)
        self.project = settings.pop("project")

        url = f"bigquery://{self.project}"
        if "dataset" in settings:
            self.dataset = settings.pop("dataset")
            url += "/" + self.dataset

        return create_engine(url, **settings)

    def _construct_tests(self, columns, table, schema=None):
        count_tests, query, breakdown = self._construct_tests_template(
            columns, table, "standard_tests_bigquery.sql", schema
        )
        if count_tests == 0:
            return Ok([None, breakdown])

        return Ok([query, breakdown])

    def test_problematic_values(self, failed: list, table: str, schema: str) -> str:
        return self.test_problematic_values_template(
            failed, table, schema, "standard_test_output_bigquery.sql"
        )

    def _list_databases(self):
        """List the accessible databases for this connection."""
        client = self.engine.raw_connection()._client
        projects = [project.project_id for project in client.list_projects()]

        return projects + [""]

    def _get_table_type(self, type):
        out_key = list()
        out_value = list()

        if type["table_type"] == "BASE TABLE":
            out_key.append("type")
            out_value.append("table")
        elif type["table_type"] == "VIEW":
            out_key.append("type")
            out_value.append("view")

        cluster_cols = []
        for c in type["columns"]:
            if c["is_partition"] is True:
                out_key.append("partition")
                out_value.append(c["column_name"])
            if c["clustering_ordinal_position"] is not None:
                cluster_cols.append(c["column_name"])
        if cluster_cols:
            out_key.append("cluster")
            out_value.append(cluster_cols)

        return dict(zip(out_key, out_value))

    def _get_schemata(self, databases):
        queries = ""
        for database in databases:
            if database == "":
                queries += """
                SELECT catalog_name, schema_name FROM INFORMATION_SCHEMA.SCHEMATA
                """
            else:
                queries += f"""
                SELECT catalog_name, schema_name FROM {database}.INFORMATION_SCHEMA.SCHEMATA
                """
            queries += "\nUNION ALL\n"

        queries = "\n".join(queries.split("\n")[:-2])
        result = self.read_data(queries)
        return result

    def _list_objects(self, databases):
        """List the accessible databases for this connection."""
        schemata = self._get_schemata(databases)
        objects = list()
        queries = ""
        for schema in schemata:
            queries += f"""SELECT '{schema['catalog_name']}' AS table_catalog
                                  , t.table_schema
                                  , t.table_name
                                  , t.table_type
                                  , array_agg(STRUCT(c.column_name, c.is_partitioning_column = 'YES' AS is_partition, c.clustering_ordinal_position)
                                              ORDER BY clustering_ordinal_position) AS columns
                               FROM {schema['catalog_name']}.{schema['schema_name']}.INFORMATION_SCHEMA.TABLES t
                               JOIN {schema['catalog_name']}.{schema['schema_name']}.INFORMATION_SCHEMA.COLUMNS c
                                 ON c.table_name = t.table_name
                              GROUP BY 1,2,3,4
                        """
            queries += "\nUNION ALL\n"

        queries = "\n".join(queries.split("\n")[:-2])

        result = self.read_data(queries)

        objects = {
            db: {
                schema: {
                    table: {"result": self._get_table_type(type) for type in ggg}.get(
                        "result"
                    )
                    for table, ggg in groupby(
                        gg,
                        lambda x: x["table_name"],
                    )
                }
                for schema, gg in groupby(g, lambda x: x["table_schema"])
            }
            for db, g in groupby(
                sorted(
                    result,
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

    def _py2sqa(self, from_type):
        python_types = {
            int: sqltypes.Integer,
            str: sqltypes.String,
            float: sqltypes.Float,
            decimal.Decimal: sqltypes.Numeric,
            datetime.datetime: sqltypes.TIMESTAMP,
            bytes: sqltypes.LargeBinary,
            bool: sqltypes.Boolean,
            datetime.date: sqltypes.Date,
            datetime.time: sqltypes.Time,
            datetime.timedelta: sqltypes.Interval,
            list: sqltypes.ARRAY,
            dict: sqltypes.JSON,
            UUID: sqltypes.String,
        }

        if from_type not in python_types:
            raise ValueError(f'Type not supported "{from_type}"')
        else:
            return python_types[from_type]().compile(dialect=self.engine.dialect)

    def _load_data_batch(self, table, data, schema):
        full_table_name = (
            f"{self.project}.{self.dataset if schema is None else schema}.{table}"
        )

        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client = self.engine.raw_connection()._client

        data_bytes = b"\n".join([orjson.dumps(d) for d in data])
        job = client.load_table_from_file(
            io.BytesIO(data_bytes), full_table_name, job_config=job_config
        )
        job.result()

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

        # ddl = self._format_properties(ddl).value

        full_src_table = f"{src_db + '.' if src_schema is not None else ''}{src_schema + '.' if src_schema is not None else ''}{src_table}"
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
            and table in self._requested_objects[db_name][schema]
        ):
            db_info = self._requested_objects[db_name][schema][table]
            object_type = db_info.get("type")
            table_exists = bool(object_type == "table")
            view_exists = bool(object_type == "view")
            partition_column = db_info.get("partition", "")
            cluster_column = set(db_info.get("cluster", set()))
        else:
            db_info = dict()
            table_exists = True
            view_exists = True
            partition_column = ""
            cluster_column = set()

        des_partitioned = ddl.get("partition") or ""
        des_clustered = set(ddl.get("cluster") or set())

        if des_clustered == cluster_column and des_partitioned == partition_column:
            drop = ""
        elif db_info.get("type") == "table":
            drop = f"DROP TABLE IF EXISTS {full_name};\n"
        else:
            drop = f"DROP VIEW IF EXISTS {full_name};\n"

        template = self._jinja_env.get_template("create_table.sql")
        query = template.render(
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
        query = drop + query
        return query


def fully_qualify(name, schema=None, db=None):
    return f"{db+'.' if db is not None else ''}{schema+'.' if schema is not None else ''}{name}"
