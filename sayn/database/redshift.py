import logging
import re

from sqlalchemy import create_engine

from .database import Database
from ..utils import yaml

db_parameters = ["host", "user", "password", "port", "dbname", "cluster_id"]


class Redshift(Database):
    sql_features = ["DROP CASCADE", "NO SET SCHEMA"]

    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

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

        engine = create_engine("postgresql://", **settings)
        self.setup_db(name, name_in_settings, db_type, engine)

    def validate_ddl(self, ddl, **kwargs):
        out_ddl = super(self, Database).validate_ddl(ddl, **kwargs)
        if out_ddl is None:
            return

        # Redshift specific ddl
        column_names = [c["name"] for c in out_ddl["columns"]]
        if "sorting" in kwargs:
            try:
                sorting = yaml.as_document(
                    kwargs["sorting"],
                    schema=yaml.Map(
                        {
                            yaml.Optional("type"): yaml.Enum(
                                ["compound", "interleaved"]
                            ),
                            "columns": yaml.UniqueSeq(
                                yaml.NotEmptyStr()
                                if len(column_names) == 0
                                else yaml.Enum(column_names)
                            ),
                        }
                    ),
                )
            except Exception as e:
                logging.error(e)
                return

            out_ddl["sorting"] = sorting.data

        if "distribution" in kwargs:
            try:
                distribution = yaml.as_document(
                    kwargs["distribution"], schema=yaml.Regex(r"even|all|key([^,]+)")
                )
            except Exception as e:
                logging.error(e)
                return

            out_ddl["distribution"] = distribution.data

        return out_ddl

    def _get_table_attributes(self, ddl):
        if "sorting" not in ddl and "distribution" not in ddl:
            return ""

        table_attributes = ""

        if "sorting" in ddl:
            sorting_type = ddl["sorting"].get("type")
            sorting_type = (
                sorting_type.upper() + " " if sorting_type is not None else ""
            )
            columns = ", ".join(ddl["sorting"]["columns"])
            table_attributes += f"\n{sorting_type}SORTKEY ({columns})"

        if "distribution" in ddl:
            if ddl["distribution"] in ("even", "all"):
                table_attributes += f"\nDISTSTYLE {ddl['distribution'].upper()}"
            else:
                column = re.match(r"key\((.*)\)", ddl["distribution"]).groups()[0]
                table_attributes += f"\nDISTSTYLE KEY\nDISTKEY ({column})"

        return table_attributes + "\n"

    def create_table_select(
        self, table, schema, select, replace=False, view=False, ddl=dict()
    ):
        """Returns SQL code for a create table from a select statment
        """
        table_name = table
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if replace:
            q += self.drop_table(table_name, schema, view) + "\n"

        q += f"CREATE {table_or_view} {table} "
        if not view:
            q += self._get_table_attributes(ddl)
        q += f" AS (\n{select}\n);"

        return q

    def create_table_ddl(self, table, schema, ddl, replace=False):
        """Returns SQL code for a create table from a select statment
        """
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
        if replace:
            q += self.drop_table(table_name, schema) + "\n"

        q += f"CREATE TABLE {table} "
        q += self._get_table_attributes(ddl)
        q += f" AS (\n      {columns}\n);"

        return q
