from collections import Counter
import datetime
import decimal
from itertools import groupby
from pathlib import Path
from typing import List, Optional, Union

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import BaseModel, validator, Extra
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import sqltypes
from sqlalchemy import text

from ..core.errors import DBError, Exc, Ok


class Hook(BaseModel):
    sql: str


class Columns(BaseModel):
    name: str
    type: Optional[str]
    dst_name: Optional[str]
    description: Optional[str]

    class Tests(BaseModel):
        name: Optional[str]
        allowed_values: Optional[List[Union[int, float, bool, str]]]
        execute: bool = True

        class Config:
            extra = Extra.forbid

    tests: List[Union[str, Tests]] = list()

    class Config:
        extra = Extra.forbid


class BaseDDL(BaseModel):
    def base_ddl(self):
        columns = list()
        for c in self.columns:
            tests = list()
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

        return {
            "columns": columns,
            "properties": list(),
            "post_hook": [h.dict() for h in self.post_hook],
        }


class DDL(BaseDDL):
    columns: List[Union[str, Columns]] = list()
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

    def get_ddl(self):
        return self.base_ddl()


class Database:
    """
    Base class for databases in SAYN.

    Databases are implemented using sqlalchemy, and the `engine` attribute is available
    when working in python tasks without the need for calling create_engine.

    Attributes:
        engine (sqlalchemy.Engine): A sqlalchemy engine referencing the database.
        name (str): Name of the db as defined in `required_credentials` in `project.yaml`.
        name_in_yaml (str): Name of db under `credentials` in `settings.yaml`.
        db_type (str): Type of the database.
        metadata (sqlalchemy.MetaData): A metadata object associated with the engine.
    """

    DDL = DDL

    def __init__(
        self,
        name,
        name_in_settings,
        db_type,
        common_params,
        settings,
    ):
        self.name = name
        self.name_in_settings = name_in_settings
        self.db_type = db_type
        self.max_batch_rows = common_params.get("max_batch_rows", 50000)
        self._settings = settings
        self._requested_objects = dict()

        self._jinja_env = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )
        self._jinja_test = Environment(
            loader=FileSystemLoader(Path(__file__).parent.parent / "tasks/tests"),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

    def _obj_str(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        return (
            f"{database if database is not None else ''}"
            f"{'.' if database is not None else ''}"
            f"{schema if schema is not None else ''}"
            f"{'.' if database is not None or (schema is not None) else ''}"
            f"{table if table is not None else ''}"
        )

    def _fully_qualify(
        self, database: Optional[str], schema: Optional[str], table: str
    ) -> str:
        return self._obj_str(database, schema, table)

    def feature(self, feature):
        # Supported sql_features
        #   - CREATE IF NOT EXISTS
        #   - CREATE TABLE NO PARENTHESES
        #   - INSERT TABLE NO PARENTHESES
        #   - DROP CASCADE
        #   - NO SET SCHEMA
        #   - NO ALTER INDEXES

        # CAN REPLACE TABLE
        # CAN REPLACE TABLE
        # CAN REPLACE VIEW
        # NEEDS CASCADE
        # CANNOT SPECIFY DDL IN SELECT
        # CANNOT ALTER INDEXES
        # CANNOT SET SCHEMA
        return feature in ()

    def create_engine(self, settings):
        raise NotImplementedError()

    def _activate_connection(self):
        self.engine = self.create_engine(self._settings)
        if self.engine is not None:
            # We'll have a None engine when the connection is missing from the settings.
            # We create said object only to allow the config stage to run correctly.
            self.metadata = MetaData(self.engine)
            # Force a query to test the connection
            self.execute("select 1")

    def _list_databases(self):
        raise NotImplementedError()

    def _list_objects(self, databases):
        """List the accessible databases for this connection."""
        objects = list()
        queries = ""
        for database in databases:
            if database != "":
                queries += f"""SELECT table_catalog, table_schema, table_name, table_type FROM {database}.INFORMATION_SCHEMA.TABLES"""
            else:
                queries += "SELECT '' AS table_catalog, table_schema, table_name, table_type FROM INFORMATION_SCHEMA.TABLES"
            queries += "\nUNION ALL\n"

        queries = "\n".join(queries.split("\n")[:-2])

        objects = {
            db.lower(): {
                schema.lower(): {
                    table.lower(): {
                        "type": self._get_table_type(type["table_type"]) for type in ggg
                    }
                    for table, ggg in groupby(gg, lambda x: x["table_name"])
                }
                for schema, gg in groupby(g, lambda x: x["table_schema"])
            }
            for db, g in groupby(
                sorted(
                    self.read_data(queries),
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

    def _get_table_type(self, type):
        if type.lower() == "table":
            return "table"
        elif type.lower() == "base table":
            return "table"
        elif type.lower() == "view":
            return "view"

    def _construct_tests_template(self, columns, table, test_file_name, schema):
        query = """
                   SELECT val
                        , col
                        , cnt
                        , type
                     FROM (
                """
        template = self._jinja_test.get_template(test_file_name)
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
                        "allowed_values": ", ".join(
                            f"{format_type(c)}" for c in t["allowed_values"]
                        ),
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
                            "allowed_values": ", ".join(
                                f"{format_type(c)}" for c in t["allowed_values"]
                            ),
                        },
                    )

        parts = query.splitlines()[:-2]
        query = ""
        for q in parts:
            query += q.strip() + "\n"
        query += ") AS t\n LIMIT 5;"

        return count_tests, query, breakdown

    def _construct_tests(self, columns, table, schema=None):
        count_tests, query, breakdown = self._construct_tests_template(
            columns, table, "standard_tests.sql", schema
        )
        if count_tests == 0:
            return Ok([None, breakdown])

        return Ok([query, breakdown])

    def _validate_ddl(self, columns, table_properties, post_hook):
        if len(columns) == 0 and len(table_properties) == 0 and len(post_hook) == 0:
            properties = self.DDL().get_ddl()
        else:
            try:
                if table_properties is None or len(table_properties) == 0:
                    properties = self.DDL(
                        columns=columns,
                        post_hook=post_hook,
                    ).get_ddl()
                else:
                    properties = self.DDL(
                        columns=columns,
                        properties=table_properties,
                        post_hook=post_hook,
                    ).get_ddl()
            except Exception as e:
                return Exc(e, db=self.name, type=self.db_type)

        return self._format_properties(properties)

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

    def test_problematic_values_template(
        self, failed: list, table: str, schema: str, test_file_name: str
    ) -> str:
        template = self._jinja_test.get_template(test_file_name)
        query = ""
        for f in failed:
            query += template.render(
                **{
                    "table": table,
                    "schema": schema,
                    "name": f[2],
                    "type": f[1],
                    "allowed_values": f[3],
                },
            )
        return query

    def test_problematic_values(self, failed: list, table: str, schema: str) -> str:
        return self.test_problematic_values_template(
            failed, table, schema, "standard_test_output.sql"
        )

    def _refresh_metadata(self, only=None, schema=None):
        """Refreshes the sqlalchemy metadata object.

        Args:
            only (list): A list of object names to filter the refresh on
            schema (str): The schema name to filter on the refresh
        """
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    def _introspect(self, objects_to_introspect):
        out = dict()
        introspect_databases = set(objects_to_introspect.keys())
        databases = set(self._list_databases())
        schemata_list = [
            (db, schem)
            for db, schs in objects_to_introspect.items()
            for schem in list(schs.keys())
        ]
        introspect_schemata = set(schemata_list)
        databases_intersection = introspect_databases.intersection(databases)
        databases_intersection.add("")  # Always introspect the default db

        if len(databases_intersection) > 0:
            dbobjects = self._list_objects(databases_intersection)
            schemata = [
                (database, schema)
                for database, schemas in dbobjects.items()
                for schema in list(schemas.keys())
            ]
            schemata = set(schemata)
            schemata_intersection = introspect_schemata.intersection(schemata)

            if len(schemata_intersection) > 0:
                for db, schema in schemata_intersection:
                    if db not in out:
                        out[db] = dict()

                    if schema not in out[db]:
                        out[db][schema] = dict()

                    if db not in dbobjects or schema not in dbobjects[db]:
                        continue

                    tables = dbobjects[db][schema]

                    for table_name in objects_to_introspect[db][schema]:
                        if table_name in tables:
                            out[db][schema][table_name] = tables[table_name]
                        else:
                            out[db][schema][table_name] = {"type": None}

        self._requested_objects = out

    def _py2sqa(self, from_type):
        python_types = {
            int: sqltypes.BigInteger,
            str: sqltypes.Unicode,
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

    # API

    def execute(self, script):
        """Executes a script in the database. Multiple statements are supported.

        Args:
            script (sql): The SQL script to execute
        """
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(text(script))

    def read_data(self, query, **params):
        """Executes the query and returns a list of dictionaries with the data.

        Args:
            query (str): The SELECT query to execute
            params (dict): sqlalchemy parameters to use when building the final query as per
                [sqlalchemy.engine.Connection.execute](https://docs.sqlalchemy.org/en/13/core/connections.html#sqlalchemy.engine.Connection.execute)

        Returns:
            list: A list of dictionaries with the results of the query

        """
        if params is not None:
            res = self.engine.execute(query, **params)
        else:
            res = self.engine.execute(query)

        return [dict(zip([str(k) for k in res.keys()], r)) for r in res.fetchall()]

    def _read_data_stream(self, query, **params):
        """Executes the query and returns an iterator dictionaries with the data.

        The main difference with read_data() is that this method executes the query with a server-side
        cursor (sqlalchemy stream_results = True).

        Args:
            query (str): The SELECT query to execute
            params (dict): sqlalchemy parameters to use when building the final query as per
                [sqlalchemy.engine.Connection.execute](https://docs.sqlalchemy.org/en/13/core/connections.html#sqlalchemy.engine.Connection.execute)

        Returns:
            list: A list of dictionaries with the results of the query

        """
        with self.engine.connect().execution_options(stream_results=True) as connection:
            res = connection.execute(query, **params)
            fields = [str(k) for k in res.keys()]

            for record in res:
                yield dict(zip(fields, record))

    def _load_data_batch(self, table, data, schema, db):
        """Implements the load of a single data batch for `load_data`.

        Defaults to an insert many statement, but it's overloaded for specific
        database connector for more efficient methods.

        Args:
            table (str): The name of the target table
            data (list): A list of dictionaries to load
            schema (str): An optional schema to reference the table
        """
        table_def = self._get_table(table, schema)
        if table_def is None:
            raise DBError(
                self.name,
                self.db_type,
                f"Table {schema + '.' if schema is not None else ''}{table} does not exists",
            )

        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(table_def.insert().values(data))

    def load_data(
        self, table, data, db=None, schema=None, batch_size=None, replace=False, **ddl
    ):
        """Loads a list of values into the database

        The default loading mechanism is an INSERT...VALUES, but database drivers
        will implement more appropriate methods.

        Args:
            table (str): The name of the target table
            data (list): A list of dictionaries to load
            schema (str): An optional schema to reference the table
            batch_size (int): The max size of each load batch. Defaults to
              `max_batch_rows` in the credentials configuration (settings.yaml)
            replace (bool): Indicates whether the target table is to be replaced
              (True) or new records are to be appended to the existing table (default)
            ddl (dict): An optional ddl specification in the same format as used
              in autosql and copy tasks

        Returns:
            int: Number of records loaded
        """
        batch_size = batch_size or self.max_batch_rows
        buffer = list()

        result = self._validate_ddl(
            ddl.get("columns", list()),
            ddl.get("table_properties", dict()),
            ddl.get("post_hook", list()),
        )

        if result.is_err:
            raise DBError(
                self.name,
                self.db_type,
                "Incorrect ddl provided",
                errors=result.error.details["errors"],
            )
        else:
            ddl = result.value

        check_create = True
        table_exists_prior_load = self._table_exists(table, schema)

        records_loaded = 0
        for i, record in enumerate(data):
            if check_create and (replace or not table_exists_prior_load):
                # Create the table if required
                if len(ddl.get("columns", list())) == 0:
                    # If no columns are specified in the ddl, figure that out
                    # based on the python types of the first record
                    columns = [
                        {"name": col, "type": self._py2sqa(type(val))}
                        for col, val in record.items()
                    ]
                    ddl = dict(ddl, columns=columns)

                query = self.create_table(
                    table, db=db, schema=schema, replace=replace, **ddl
                )
                self.execute(query)
                check_create = False

            if i % batch_size == 0 and len(buffer) > 0:
                self._load_data_batch(table, buffer, schema, db)
                records_loaded += len(buffer)
                buffer = list()

            buffer.append(record)

        if len(buffer) > 0:
            self._load_data_batch(table, buffer, schema, db)
            records_loaded += len(buffer)

        return records_loaded

    def _get_table(self, table, schema):
        """Create a SQLAlchemy Table object.

        Args:
            table (str): The table name
            schema (str): The schema or None

        Returns:
            sqlalchemy.Table: A table object from sqlalchemy
        """
        table_def = Table(table, self.metadata, schema=schema, extend_existing=True)

        if table_def.exists():
            table_def = Table(
                table, self.metadata, schema=schema, extend_existing=True, autoload=True
            )
            return table_def

    def _table_exists(self, table, schema):
        return self._get_table(table, schema) is not None

    # =========
    # ETL steps
    # =========

    # Intermediary steps

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
            and table in self._requested_objects[db_name][schema_name]
        ):
            object_type = self._requested_objects[db_name][schema_name][table].get(
                "type"
            )
            table_exists = object_type == "table"
            view_exists = object_type == "view"
        else:
            table_exists = True
            view_exists = True

        template = self._jinja_env.get_template("create_table.sql")

        return template.render(
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

        template = self._jinja_env.get_template("merge_tables.sql")
        return template.render(
            dst_table=dst_table,
            src_table=src_table,
            cleanup=True,
            delete_key=delete_key,
        )

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
        template = self._jinja_env.get_template("move_table.sql")
        dst_dbs = dst_db or ""
        dst_schemas = dst_schema or ""

        if (
            dst_dbs in self._requested_objects
            and dst_schemas in self._requested_objects[dst_dbs]
            and dst_table in self._requested_objects[dst_dbs][dst_schemas]
        ):
            object_type = self._requested_objects[dst_dbs][dst_schemas][dst_table].get(
                "type"
            )

            table_exists = bool(object_type == "table")
            view_exists = bool(object_type == "view")
        else:
            table_exists = True
            view_exists = True

        return template.render(
            table_exists=table_exists,
            view_exists=view_exists,
            src_db=src_db,
            src_schema=src_schema,
            src_table=src_table,
            dst_db=dst_db,
            dst_schema=dst_schema,
            dst_table=dst_table,
            cannot_alter_indexes=self.feature("CANNOT ALTER INDEXES"),
            needs_cascade=self.feature("NEEDS CASCADE"),
            rename_changes_schema=self.feature("TABLE RENAME CHANGES SCHEMA"),
            **ddl,
        )

    # ETL steps

    def replace_table(
        self,
        table,
        select,
        schema=None,
        db=None,
        tmp_schema=None,
        tmp_db=None,
        **ddl,
    ):
        # Create the temporary table
        can_replace_table = self.feature("CAN REPLACE TABLE")

        tmp_table = tmp_name(table)
        tmp_schema = tmp_schema or schema
        tmp_db = tmp_db or db

        if can_replace_table:
            create_or_replace = self.create_table(
                table, schema, db, select=select, replace=True, **ddl
            )

            return {"Create Or Replace Table": create_or_replace}

        else:
            create_or_replace = self.create_table(
                tmp_table, tmp_schema, tmp_db, select=select, replace=True, **ddl
            )

            # Move the table to its final location
            move = self.move_table(
                tmp_table,
                table,
                src_schema=tmp_schema or schema,
                dst_schema=schema,
                **ddl,
            )

            return {"Create Table": create_or_replace, "Move table": move}

    def replace_view(self, view, select, schema=None, db=None, **ddl):
        view_name = fully_qualify(view, schema, db)
        dbs = db or ""
        schemas = schema or ""
        if (
            dbs in self._requested_objects
            and schemas in self._requested_objects[dbs]
            and view in self._requested_objects[dbs][schemas]
        ):
            object_type = self._requested_objects[dbs][schemas][view].get("type")
            table_exists = object_type == "table"
            view_exists = object_type == "view"
        else:
            table_exists = True
            view_exists = True

        # ddl = self._format_properties(ddl).value

        template = self._jinja_env.get_template("create_view.sql")
        create = template.render(
            table_name=view_name,
            view_exists=view_exists,
            table_exists=table_exists,
            select=select,
            can_replace_view=self.feature("CAN REPLACE VIEW"),
            needs_cascade=self.feature("NEEDS CASCADE"),
            **ddl,
        )
        return {"Create View": create}

    def merge_query(
        self, table, select, delete_key, schema=None, db=None, tmp_schema=None, **ddl
    ):
        tmp_table = tmp_name(table)
        tmp_schema = tmp_schema or schema
        tmp_db = db

        create_or_replace = self.create_table(
            tmp_table, tmp_schema, tmp_db, select=select, replace=True
        )

        merge = self.merge_tables(
            tmp_table,
            table,
            delete_key,
            cleanup=True,
            src_schema=tmp_schema,
            dst_schema=schema,
            src_db=db,
            dst_db=db,
        )

        return {"Create Table": create_or_replace, "Merge Tables": merge}

    def get_db_object(self, database, schema, object):
        return self._object_builder.from_components(database, schema, object)


def fully_qualify(name, schema=None, db=None):
    return f"{db+'.' if db is not None else ''}{schema+'.' if schema is not None else ''}{name}"


def tmp_name(name):
    return f"sayn_tmp_{name}"


def format_type(value):
    if isinstance(value, str):
        return f"'{value}'"
    else:
        return value
