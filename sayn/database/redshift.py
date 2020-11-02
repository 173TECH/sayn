from typing import List, Optional

from pydantic import BaseModel, constr, validator
from sqlalchemy import create_engine

from ..core.errors import DBError
from . import Database, DDL

db_parameters = ["host", "user", "password", "port", "dbname", "cluster_id"]


class RedshiftDDL(DDL):
    class Sorting(BaseModel):
        type: Optional[str]
        columns: List[str]

        @validator("type")
        def validate_type(cls, v, values):
            if v.upper() not in ("COMPOUND", "INTERLEAVED"):
                raise ValueError(
                    'Sorting type must be one of "COMPOUND" or "INTERLEAVED"'
                )

            return v.upper()

    distribution: Optional[constr(regex=r"even|all|key([^,]+)")]
    sorting: Optional[Sorting]

    @validator("indexes")
    def index_columns_exists(cls, v, values):
        raise ValueError("Indexes not supported by Redshift")

    @validator("distribution")
    def validate_distribution(cls, v, values):
        distribution = {"type": v.upper() if v in ("even", "all") else "KEY"}
        if distribution["type"] == "KEY":
            distribution["column"] = v[len("key(") : -1]
            if (
                len(values["columns"]) > 0
                and distribution["column"] not in values["columns"]
            ):
                raise ValueError(
                    f'Distribution key "{distribution["column"]}" is not declared in columns'
                )

        return distribution

    def get_ddl(self):
        return {
            "columns": [
                {"name": c} if isinstance(c, str) else c.dict() for c in self.columns
            ],
            "indexes": {k: v.dict() for k, v in self.indexes.items()},
            "permissions": self.permissions,
            "distribution": self.distribution,
            "sorting": self.sorting.dict() if self.sorting is not None else None,
        }


class Redshift(Database):
    ddl_validation_class = RedshiftDDL

    sql_features = ["DROP CASCADE", "NO SET SCHEMA"]

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

            import boto3

            boto_session = boto3.Session()

            redshift_client = boto_session.client("redshift")

            # Get the address when not provided
            if host is None or port is None:
                cluster_info = redshift_client.describe_clusters(
                    ClusterIdentifier=cluster_id
                )["Clusters"][0]

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

    def create_table_select(
        self, table, schema, select, view=False, ddl=dict(), execute=True
    ):
        """Returns SQL code for a create table from a select statment
        """
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

    def create_table_ddl(self, table, schema, ddl, execute=False):
        """Returns SQL code for a create table from a select statment
        """
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
