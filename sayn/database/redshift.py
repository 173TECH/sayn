from collections import Counter
import csv
from pathlib import Path
import tempfile
from typing import List, Optional, Union

from pydantic import BaseModel, constr, validator, Extra
from sqlalchemy import create_engine

from ..core.errors import DBError
from . import Database, Columns, Hook, BaseDDL


db_parameters = [
    "host",
    "user",
    "password",
    "port",
    "dbname",
    "cluster_id",
    "bucket",
    "bucket_region",
    "role",
    "profile",
]

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
    def validate_distribution(cls, v, values):
        if v is not None:
            if v.distribution is not None:
                distribution = {
                    "type": v.distribution.upper()
                    if v.distribution.lower() in ("even", "all")
                    else "KEY"
                }
                if distribution["type"] == "KEY":
                    distribution["column"] = v.distribution[len("key(") : -1]
                    if len(values.get("columns")) > 0 and distribution[
                        "column"
                    ] not in values.get("columns"):
                        raise ValueError(
                            f'Distribution key "{distribution["column"]}" is not declared in columns'
                        )
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
    bucket_region = None

    def feature(self, feature):
        return feature in (
            "NEEDS CASCADE",
            "CAN REPLACE VIEW",
            "CANNOT CHANGE SCHEMA",
            "CANNOT SPECIFY DDL IN SELECT",
        )

    def create_engine(self, settings):
        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()

        if "cluster_id" in settings and "password" not in settings:
            # if a cluster_id is given and no password, we use boto to complete the credentials
            cluster_id = settings.pop("cluster_id")

            host = settings.pop("host", None)
            port = settings.pop("port", None)
            # User is required
            user = settings.pop("user")
            profile = settings.pop("profile")
            self.bucket = settings.pop("bucket")
            self.bucket_region = settings.pop("bucket_region")
            self.role = settings.pop("role")

            import boto3

            self._boto_session = boto3.Session(profile_name=profile)

            redshift_client = self._boto_session.client("redshift")

            # Get the address when not provided
            if host is None or port is None:
                try:
                    cluster_info = redshift_client.describe_clusters(
                        ClusterIdentifier=cluster_id
                    )["Clusters"][0]
                except:
                    raise DBError(
                        self.name,
                        self.db_type,
                        f"Error retrieving information for cluster: {cluster_id}",
                    )

                settings["connect_args"]["host"] = (
                    host or cluster_info["Endpoint"]["Address"]
                )
                settings["connect_args"]["port"] = (
                    port or cluster_info["Endpoint"]["Port"]
                )

            # Get the password and user
            credentials = redshift_client.get_cluster_credentials(
                ClusterIdentifier=cluster_id, DbUser=user
            )

            settings["connect_args"]["user"] = credentials["DbUser"]
            settings["connect_args"]["password"] = credentials["DbPassword"]

        for param in db_parameters:
            if param in settings:
                settings["connect_args"][param] = settings.pop(param)

        return create_engine("postgresql://", **settings)

    def _get_table_attributes(self, ddl):

        if ddl["sorting"] is None and ddl["distribution"] is None:
            return ""

        table_attributes = ""

        if ddl["sorting"] is not None:
            sorting_type = (
                f"{ddl['sorting']['type']} "
                if ddl["sorting"]["type"] is not None
                else ""
            )
            columns = ", ".join(ddl["sorting"]["columns"])
            table_attributes += f"\n{sorting_type}SORTKEY ({columns})"

        if ddl["distribution"] is not None:
            if ddl["distribution"] in ("EVEN", "ALL"):
                table_attributes += f"\nDISTSTYLE {ddl['distribution']['type']}"
            else:
                table_attributes += (
                    f"\nDISTSTYLE KEY\nDISTKEY ({ddl['distribution']['column']})"
                )

        return table_attributes + "\n"

    def _create_table_select(
        self, table, schema, select, view=False, ddl=dict(), execute=True
    ):
        """Returns SQL code for a create table from a select statement"""
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""

        q += f"CREATE {table_or_view} {table} "
        if not view:
            q += self._get_table_attributes(ddl)
        q += f" AS (\n{select}\n);"

        if execute:
            self.execute(q)

        return q

    def _create_table_ddl(self, table, schema, ddl, execute=False):
        """Returns SQL code for a create table from a select statement"""
        if len(ddl["columns"]) == 0:
            raise DBError(
                self.name, self.db_type, "DDL is missing columns specification"
            )
        table_name = table
        table = f"{schema+'.' if schema else ''}{table_name}"

        columns = "\n    , ".join(
            [
                (
                    f'{c["name"]} {c["type"]}'
                    f'{" NOT NULL" if c.get("not_null", False) else ""}'
                )
                for c in ddl["columns"]
            ]
        )

        q = ""

        q += f"CREATE TABLE {table} "
        q += self._get_table_attributes(ddl)
        q += f" AS (\n      {columns}\n);"

        if execute:
            self.execute(q)

        return q

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
        template = self._jinja_env.get_template("redshift_load_batch.sql")
        fname = "batch.csv"

        iam_client = self._boto_session.client("iam")
        arn = iam_client.get_role(RoleName=self.role)["Role"]["Arn"]

        s3_client = self._boto_session.client("s3")

        with tempfile.TemporaryDirectory() as tmpdirname:
            with (Path(tmpdirname) / fname).open("w") as f:
                writer = csv.DictWriter(
                    f, fieldnames=data[0].keys(), delimiter="|", escapechar="\\"
                )
                writer.writeheader()
                writer.writerows(data)

            with (Path(tmpdirname) / fname).open("rb") as f:
                s3_client.upload_fileobj(f, self.bucket, fname)

            self.execute(
                template.render(
                    full_table_name=full_table_name,
                    temp_file_directory=tmpdirname,
                    temp_file_name=fname,
                    arn=arn,
                    bucket=self.bucket,
                    region=self.bucket_region,
                )
            )
