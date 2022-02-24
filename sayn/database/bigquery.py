from copy import deepcopy
from collections import Counter
import csv
import datetime
import decimal
import io
import json
from typing import List, Optional, Union

from pydantic import validator, Extra, BaseModel
from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from . import Database, Columns, Hook

from ..core.errors import Ok

db_parameters = ["project", "credentials_path", "location", "dataset"]


class DDL(BaseModel):
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
        columns = list()
        for c in self.columns:
            tests = []
            for t in c.tests:
                if isinstance(t, str):
                    tests.append({"type": t, "allowed_values": [], "execute": True})
                else:
                    tests.append(
                        {
                            "type": t.name if t.name is not None else "allowed_values",
                            "allowed_values": t.allowed_values
                            if t.allowed_values is not None
                            else [],
                            "execute": t.execute,
                        }
                    )
            columns.append(
                {
                    "name": c.name,
                    "description": c.description,
                    "dst_name": c.dst_name,
                    "type": c.type,
                    "tests": tests,
                }
            )

        res = {
            "columns": columns,
            "properties": list(),
            "post_hook": [h.dict() for h in self.post_hook],
        }

        properties = list()
        if self.properties is not None:
            if self.properties.cluster is not None:
                properties.append({"cluster": self.properties.cluster})
                res["cluster"] = self.properties.cluster

            if self.properties.partition is not None:
                properties.append({"partition": self.properties.partition})
                res["partition"] = self.properties.partition

        return res


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
        query = """
                   SELECT val
                        , col
                        , cnt
                        , type
                     FROM (
                """
        template = self._jinja_test.get_template("standard_tests_bigquery.sql")
        count_tests = 0
        breakdown = []
        for col in columns:
            tests = col["tests"]
            for t in tests:
                breakdown.append(
                    {
                        "column": col["name"]
                        if not col["dst_name"]
                        else col["dst_name"],
                        "type": t["type"],
                        "execute": t["execute"],
                    }
                )
                if t["execute"]:
                    count_tests += 1
                    query += template.render(
                        **{
                            "table": table,
                            "schema": schema,
                            "name": col["name"]
                            if not col["dst_name"]
                            else col["dst_name"],
                            "type": t["type"],
                            "allowed_values": ", ".join(f"'{c}'" for c in t["values"]),
                        },
                    )

        parts = query.splitlines()[:-2]
        query = ""
        for q in parts:
            query += q.strip() + "\n"
        query += ") AS t;"

        if count_tests == 0:
            return Ok([None, breakdown])

        return Ok([query, breakdown])

    def _format_properties(self, properties):
        if properties["columns"]:
            columns = []
            for col in properties["columns"]:
                entry = {
                    "name": col["name"],
                    "type": col["type"],
                    "dst_name": col["dst_name"],
                    "unique": False,
                    "not_null": False,
                    "allowed_values": False,
                }
                if "tests" in col:
                    entry.update({"tests": col["tests"]})
                    for t in col["tests"]:
                        if t["type"] != "values" and col["type"]:
                            entry.update({t["type"]: True})
                columns.append(entry)

            properties["columns"] = columns

        return Ok(properties)

    def _introspect(self, to_introspect):
        for project, datasets in to_introspect.items():
            if project != "":
                # We currently don't support 3 levels of db object specification.
                raise ValueError("3 level db objects are not currently supported")

            for dataset, objects in datasets.items():
                if dataset is None or dataset == "":
                    name = self.dataset
                else:
                    name = dataset

                query = f"""SELECT t.table_name AS name
                                  , t.table_type AS type
                                  , array_agg(STRUCT(c.column_name, c.is_partitioning_column = 'YES' AS is_partition, c.clustering_ordinal_position)
                                              ORDER BY clustering_ordinal_position) AS columns
                               FROM {name}.INFORMATION_SCHEMA.TABLES t
                               JOIN {name}.INFORMATION_SCHEMA.COLUMNS c
                                 ON c.table_name = t.table_name
                              WHERE t.table_name IN ({', '.join(f"'{ts}'" for ts in objects)})
                              GROUP BY 1,2
                        """
                db_objects = {
                    o["name"]: {"type": o["type"], "columns": o["columns"]}
                    for o in self.read_data(query)
                }

                if dataset not in self._requested_objects:
                    self._requested_objects[dataset] = dict()

                for obj_name in objects:
                    # Always insert into the requested_objects dict
                    self._requested_objects[dataset][obj_name] = {"type": None}

                    # Get the current config on the db
                    if obj_name in db_objects:
                        db_object = db_objects[obj_name]
                        if db_object["type"] == "BASE TABLE":
                            self._requested_objects[dataset][obj_name]["type"] = "table"
                        elif db_object["type"] == "VIEW":
                            self._requested_objects[dataset][obj_name]["type"] = "view"

                        cluster_cols = []
                        for c in db_object["columns"]:
                            if c["is_partition"] is True:
                                self._requested_objects[dataset][obj_name][
                                    "partition"
                                ] = c["column_name"]
                            if c["clustering_ordinal_position"] is not None:
                                cluster_cols.append(c["column_name"])
                        if cluster_cols:
                            self._requested_objects[dataset][obj_name][
                                "cluster"
                            ] = cluster_cols

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
        }

        if from_type not in python_types:
            raise ValueError(f'Type not supported "{from_type}"')
        else:
            return python_types[from_type]().compile(dialect=self.engine.dialect)

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

        # job_config = bigquery.LoadJobConfig(
        #     source_format=bigquery.SourceFormat.CSV,
        #     skip_leading_rows=1,
        #     # autodect=True,
        # )

        # client = self.engine.raw_connection()._client
        # job = client.load_table_from_file(
        #     buffer, full_table_name, job_config=job_config
        # )
        # job.result()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client = self.engine.raw_connection()._client

        def default(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            elif isinstance(o, decimal.Decimal):
                return f"{o}"
            else:
                raise ValueError("Unsuported type")

        data_str = "\n".join([json.dumps(d, default=default) for d in data])
        job = client.load_table_from_file(
            io.StringIO(data_str), full_table_name, job_config=job_config
        )
        job.result()

    def move_table(self, src_table, dst_table, src_schema=None, dst_schema=None, **ddl):

        # ddl = self._format_properties(ddl).value

        full_src_table = (
            f"{src_schema + '.' if src_schema is not None else ''}{src_table}"
        )
        select = f"SELECT * FROM {full_src_table}"
        create_or_replace = self.create_table(
            dst_table, dst_schema, select=select, replace=True, **ddl
        )

        return "\n\n".join((create_or_replace, f"DROP TABLE {full_src_table}"))

    def create_table(
        self,
        table,
        schema=None,
        select=None,
        replace=False,
        **ddl,
    ):
        full_name = fully_qualify(table, schema)
        if (
            schema in self._requested_objects
            and table in self._requested_objects[schema]
        ):
            db_info = self._requested_objects[schema][table]
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


def fully_qualify(name, schema=None):
    return f"{schema+'.' if schema is not None else ''}{name}"
