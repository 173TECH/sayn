from copy import deepcopy
import csv
import datetime
import decimal
import io
import json
from typing import List, Optional

from pydantic import validator, Extra
from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from . import Database, DDL

db_parameters = ["project", "credentials_path", "location", "dataset"]


class BigqueryDDL(DDL):
    partition: Optional[str]
    cluster: Optional[List[str]]

    class Config:
        extra = Extra.forbid

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

        des_clustered = ddl["cluster"]
        des_partitioned = ddl["partition"]

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
