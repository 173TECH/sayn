import logging
import re

from sqlalchemy import create_engine

from .database import Database
from ..utils import yaml

db_parameters = ["host", "user", "password", "port", "dbname"]


class Redshift(Database):
    sql_features = ["DROP CASCADE", "NO SET SCHEMA"]

    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()
        for param in db_parameters:
            if param in settings:
                settings["connect_args"][param] = settings.pop(param)

        engine = create_engine("postgresql://", **settings)
        self.setup_db(name, name_in_settings, db_type, engine)

    def validate_ddl(self, ddl, type_required=True):
        schema = yaml.Map(
            {
                yaml.Optional("columns"): yaml.Seq(
                    yaml.Map(
                        {
                            "name": yaml.NotEmptyStr(),
                            "type"
                            if type_required
                            else yaml.Optional("type"): yaml.NotEmptyStr(),
                            yaml.Optional("primary"): yaml.Bool(),
                            yaml.Optional("not_null"): yaml.Bool(),
                            yaml.Optional("unique"): yaml.Bool(),
                        }
                    )
                ),
                yaml.Optional("indexes"): yaml.MapPattern(
                    yaml.NotEmptyStr(),
                    yaml.Map({"columns": yaml.UniqueSeq(yaml.NotEmptyStr())}),
                ),
                yaml.Optional("distribution"): yaml.Regex(r"even|all|key([^,]+)"),
                yaml.Optional("sorting"): yaml.Map(
                    {
                        yaml.Optional("type"): yaml.Enum(["compound", "interleaved"]),
                        "columns": yaml.UniqueSeq(yaml.NotEmptyStr()),
                    }
                ),
                yaml.Optional("permissions"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.NotEmptyStr()
                ),
            }
        )

        try:
            ddl.revalidate(schema)
        except Exception as e:
            logging.error(e)
            return

        return ddl.data

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
