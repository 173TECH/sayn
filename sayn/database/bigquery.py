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

from ..core.errors import DBError, Exc, Ok

db_parameters = ["project", "credentials_path", "location", "dataset"]


class Bigquery(Database):
    class DDL(BaseModel):
        class Properties(BaseModel):
            partition: Optional[str]
            cluster: Optional[List[str]]

            class Config:
                extra = Extra.forbid

        columns: Optional[List[Columns]] = list()
        properties: Optional[List[Properties]] = list()
        post_hook: Optional[List[Hook]] = list()

        class Config:
            extra = Extra.forbid

        @validator("columns", pre=True)
        def transform_str_cols(cls, v, values):
            if v is not None and isinstance(v, List):
                return [{"name": c} if isinstance(c, str) else c for c in v]
            else:
                return v

        @validator("columns")
        def columns_unique(cls, v, values):
            dupes = {k for k, v in Counter([e.name for e in v]).items() if v > 1}
            if len(dupes) > 0:
                raise ValueError(f"Duplicate columns: {','.join(dupes)}")
            else:
                return v

        @validator("properties")
        def validate_cluster(cls, v, values):
            if len(values.get("columns")) > 0:
                missing_columns = set(v) - set([c.name for c in values.get("columns")])
                if len(missing_columns) > 0:
                    raise ValueError(
                        f'Cluster contains columns not specified in the ddl: "{missing_columns}"'
                    )

            return v

        def get_ddl(self):
            cols = self.columns
            self.columns = []
            for c in cols:
                tests = []
                for t in c.tests:
                    if isinstance(t, str):
                        tests.append({"type": t, "values": []})
                    else:
                        tests.append(
                            {
                                "type": t.name if t.name is not None else "values",
                                "values": t.values if t.values is not None else [],
                            }
                        )
                self.columns.append(
                    {
                        "name": c.name,
                        "description": c.description,
                        "dst_name": c.dst_name,
                        "type": c.type,
                        "tests": tests,
                    }
                )

            props = self.properties
            self.properties = []
            for p in props:
                self.properties.append(p.dict())

            hook = self.post_hook
            self.post_hook = []
            for h in hook:
                self.post_hook.append(h.dict())

            res = {
                "columns": self.columns,
                "properties": self.properties,
                "post_hook": self.post_hook,
            }

            return res

    # ddl_validation_class = DDL

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
                   SELECT col
                        , cnt
                        , type
                     FROM (
                """
        template = self._jinja_test.get_template("standard_tests_bigquery.sql")
        count_tests = 0
        for col in columns:
            tests = col["tests"]
            for t in tests:
                if col[t["type"]] is False:
                    count_tests += 1
                    query += template.render(
                        **{
                            "table": table,
                            "schema": schema,
                            "name": col["name"]
                            if not col["dst_name"]
                            else col["dst_name"],
                            "type": t["type"],
                            "values": ", ".join(f"'{c}'" for c in t["values"]),
                        },
                    )
        parts = query.splitlines()[:-2]
        query = ""
        for q in parts:
            query += q.strip() + "\n"
        query += ") AS t;"

        if count_tests == 0:
            return Ok("")

        return Ok(query)

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
                    "values": False,
                }
                if "tests" in col:
                    entry.update({"tests": col["tests"]})
                    for t in col["tests"]:
                        if t["type"] != "values" and t["type"] != "unique":
                            entry.update({t["type"]: True})
                columns.append(entry)

            properties["columns"] = columns
        if properties["properties"]:
            for pro in properties["properties"]:
                for p in pro:
                    if pro[p] is not None:
                        properties[p] = pro[p]

        return Ok(properties)

    def _introspect(self):
        for schema in self._requested_objects.keys():
            obj = [obj_name for obj_name in self._requested_objects[schema]]
            if schema is None:
                name = self.dataset
            else:
                name = schema
            query = f"""SELECT t.table_name
                              , t.table_type
                              , array_agg(STRUCT(c.column_name, c.is_partitioning_column = 'YES' AS is_partition, c.clustering_ordinal_position)
                                          ORDER BY clustering_ordinal_position) AS columns
                           FROM {name}.INFORMATION_SCHEMA.TABLES t
                           JOIN {name}.INFORMATION_SCHEMA.COLUMNS c
                             ON c.table_name = t.table_name
                          WHERE t.table_name IN ({', '.join(f"'{ts}'" for ts in obj)})
                          GROUP BY 1,2
                    """
            res = self.read_data(query)

            for obj in res:
                obj_name = obj["table_name"]
                if obj["table_type"] == "BASE TABLE":
                    obj_type = "table"
                elif obj["table_type"] == "VIEW":
                    obj_type = "view"
                if schema is not None and obj_name.startswith(schema + "."):
                    obj_name = obj_name[len(schema + ".") :]
                if obj_name in self._requested_objects[schema]:
                    self._requested_objects[schema][obj_name]["type"] = obj_type
                    cols = []
                    for c in obj["columns"]:
                        if c["is_partition"] is True:
                            self._requested_objects[schema][obj_name]["partition"] = c[
                                "column_name"
                            ]
                        if c["clustering_ordinal_position"] is not None:
                            cols.append(c["column_name"])
                    if cols:
                        self._requested_objects[schema][obj_name]["cluster"] = cols

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
            object_type = self._requested_objects[schema][table].get("type")
            table_exists = bool(object_type == "table")
            view_exists = bool(object_type == "view")
            if "partition" in self._requested_objects[schema][table].keys():
                partition_column = self._requested_objects[schema][table]["partition"]
            else:
                partition_column = None
            if "cluster" in self._requested_objects[schema][table].keys():
                cluster_column = self._requested_objects[schema][table]["cluster"]
            else:
                cluster_column = None
        else:
            table_exists = True
            view_exists = True
            partition_column = None
            cluster_column = None

        try:
            des_clustered = ddl["cluster"]
        except:
            des_clustered = ""
        try:
            des_partitioned = ddl["partition"]
        except:
            des_partitioned = ""

        if des_clustered == cluster_column and des_partitioned == partition_column:
            drop = ""
        else:
            drop = f"DROP TABLE IF EXISTS { table };"

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
