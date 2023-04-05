from collections import Counter
import csv
from io import BytesIO
import gzip
from pathlib import Path
from typing import List, Optional, Union

import orjson
from pydantic import BaseModel, constr, validator, Extra
from sqlalchemy import create_engine

from ..core.errors import DBError
from . import Database, Columns, Hook, BaseDDL


DistributionStr = constr(regex=r"even|all|key([^,]+)")


class DDL(BaseDDL):
    class Properties(BaseModel):
        class Sorting(BaseModel):
            type: Optional[str]
            columns: List[str]

            class Config:
                extra = Extra.forbid

            @validator("type")
            def validate_type(cls, v):
                if v.upper() not in ("COMPOUND", "INTERLEAVED"):
                    raise ValueError(
                        'Sorting type must be one of "COMPOUND" or "INTERLEAVED"'
                    )

                return v.upper()

        distribution: Optional[DistributionStr]
        sorting: Optional[Sorting]

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
    def validate_distribution(cls, v):
        if v is not None:
            if v.distribution is not None:
                distribution = {
                    "type": v.distribution.upper()
                    if v.distribution.lower() in ("even", "all")
                    else "KEY"
                }
                if distribution["type"] == "KEY":
                    distribution["column"] = v.distribution[len("key") + 1 : -1]
                v.distribution = distribution

            return v
        else:
            return v

    def get_ddl(self):
        result = self.base_ddl()
        properties = list()
        if self.properties is not None:
            if self.properties.distribution is not None:
                properties.append({"distribution": self.properties.distribution})
                result["distribution"] = self.properties.distribution

            if self.properties.sorting is not None:
                properties.append({"sorting": self.properties.sorting})
                result["sorting"] = self.properties.sorting

        return result


class Redshift(Database):
    DDL = DDL
    _boto_session = None
    bucket = None
    access_key_id = None
    secret_access_key = None
    session_token = None
    profile = None

    def feature(self, feature):
        return feature in (
            "NEEDS CASCADE",
            "CANNOT CHANGE SCHEMA",
            "CANNOT SPECIFY DDL IN SELECT",
        )

    def create_engine(self, settings):
        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()

        region = settings.pop("region", None)

        if "cluster_id" in settings and "password" not in settings:
            # if a cluster_id is given and no password, we use boto to
            # complete the credentials
            settings["connect_args"]["iam"] = True
            settings["connect_args"]["db_user"] = settings.pop("user")

            settings["connect_args"]["cluster_identifier"] = settings.pop("cluster_id")

            if "aws_access_key_id" in settings or "aws_secret_access_key" in settings:
                self.access_key_id = settings.pop("aws_access_key_id", None)
                self.secret_access_key = settings.pop("aws_secret_access_key", None)
                self.session_token = settings.pop("aws_session_token", None)

                if self.access_key_id is None or self.secret_access_key is None:
                    raise DBError(
                        self.name,
                        self.db_type,
                        "Error retrieving AWS access key credentials",
                    )
            elif "profile" in settings:
                self.profile = settings.pop("profile")

        if "bucket" in settings:
            self.bucket = {"name": None, "region": None, "role": None}
            bucket = settings.pop("bucket")
            if isinstance(bucket, str):
                self.bucket["name"] = bucket
            elif isinstance(bucket, dict):
                if "name" in bucket:
                    self.bucket["name"] = bucket["name"]

                if "region" in bucket:
                    self.bucket["region"] = bucket["region"]

                if "role" in bucket:
                    self.bucket["role"] = bucket["role"]

            # Setup the boto3 session
            import boto3

            self._boto_session = boto3.Session(
                profile_name=self.profile,
                region_name=region,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                aws_session_token=self.session_token,
            )

        dbname = settings.pop("dbname")

        db_parameters = [
            "host",
            "user",
            "password",
            "port",
        ]

        for param in db_parameters:
            if param in settings:
                settings["connect_args"][param] = settings.pop(param)

        if self.access_key_id is not None:
            settings["connect_args"]["access_key_id"] = self.access_key_id

        if self.secret_access_key is not None:
            settings["connect_args"]["secret_access_key"] = self.secret_access_key

        if self.session_token is not None:
            settings["connect_args"]["session_token"] = self.session_token

        if self.profile is not None:
            settings["connect_args"]["profile"] = self.profile

        return create_engine(f"redshift+redshift_connector:///{dbname}", **settings)

    def execute(self, script):
        conn = self.engine.raw_connection()
        with conn.cursor() as cursor:
            for s in script.split(";"):
                if len(s.strip()) > 0:
                    cursor.execute(s)
                    cursor.execute("COMMIT")

    def _list_databases(self):
        report = self.read_data("SELECT datname FROM pg_database")
        dbs = [re["datname"] for re in report]
        return dbs

    def _load_data_batch(self, table, data, schema, db):
        """Implements the load of a single data batch for `load_data`.

        Defaults to an insert many statement, but it's overloaded for specific
        database connector for more efficient methods.

        Args:
            table (str): The name of the target table
            data (list): A list of dictionaries to load
            schema (str): An optional schema to reference the table
        """
        # if no bucket is supplied, the old _load_data_batch function is used
        if self.bucket is None:
            return super(Database, self)._load_data_batch(table, data, schema, db)

        full_table_name = f"{'' if db is None else db + '.'}{'' if schema is None else schema + '.'}{table}"
        template = self._jinja_env.get_template("redshift_load_batch.sql")

        s3_client = self._boto_session.client("s3", region_name=self.bucket["region"])

        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="w") as gf:
            gf.write(b"\n".join([orjson.dumps(r, default=str) for r in data]))

        buf.seek(0)
        fname = "batch.json.gz"
        s3_client.upload_fileobj(buf, self.bucket["name"], fname)

        self.execute(
            template.render(
                full_table_name=full_table_name,
                temp_file_name=fname,
                bucket=self.bucket["name"],
                region=self.bucket["region"],
            )
        )

    def merge_tables(
        self,
        src_table,
        dst_table,
        delete_key,
        cleanup=True,
        src_schema=None,
        dst_schema=None,
        src_db=None,
        dst_db=None,
        **ddl,
    ):
        src_table = fully_qualify(src_table, src_schema, src_db)
        dst_table = fully_qualify(dst_table, dst_schema, dst_db)

        template = self._jinja_env.get_template("redshift_merge_tables.sql")
        return template.render(
            dst_table=dst_table,
            src_table=src_table,
            cleanup=True,
            delete_key=delete_key,
        )


def fully_qualify(name, schema=None, db=None):
    return f"{db+'.' if db is not None else ''}{schema+'.' if schema is not None else ''}{name}"
