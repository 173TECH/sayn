from collections import Counter
import datetime
import decimal
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pydantic import BaseModel, validator, conlist
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes

from ..core.errors import DBError, Exc, Ok

from ..utils.misc import group_list


class DDL(BaseModel):
    class Column(BaseModel):
        name: str
        type: Optional[str]
        primary: Optional[bool] = False
        not_null: Optional[bool] = False
        unique: Optional[bool] = False

    class Index(BaseModel):
        columns: conlist(str, min_items=1)

    columns: Optional[List[Column]] = list()
    indexes: Optional[Dict[str, Index]] = dict()
    # logic field - i.e. not added by the user in the ddl definition
    primary_key: Optional[List[str]] = []

    permissions: Optional[Dict[str, str]] = dict()

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

    @validator("indexes")
    def index_columns_exists(cls, v, values):
        cols = [c.name for c in values.get("columns", list())]
        if len(cols) > 0:
            missing_cols = group_list(
                [
                    (index_name, index_column)
                    for index_name, index in v.items()
                    for index_column in index.columns
                    if index_column not in cols
                ]
            )
            if len(missing_cols) > 0:
                cols_msg = ";".join(
                    [f"On {i}: {','.join(c)}" for i, c in missing_cols.items()]
                )
                raise ValueError(f"Some indexes refer to missing columns: {cols_msg}")

        return v

    @validator("primary_key", always=True)
    def set_pk(cls, v, values):
        columns_pk = [c.name for c in values.get("columns", []) if c.primary]

        indexes_pk = list()
        if values.get("indexes", {}).get("primary_key") is not None:
            indexes_pk = values.get("indexes").get("primary_key").columns

        if len(columns_pk) > 0 and len(indexes_pk) > 0:
            if set(columns_pk) != set(indexes_pk):
                columns_pk_str = " ,".join(columns_pk)
                indexes_pk_str = " ,".join(indexes_pk)
                raise ValueError(
                    f"Primary key defined in indexes ({indexes_pk_str}) does not match primary key defined in columns ({columns_pk_str})."
                )

        pk = columns_pk if len(columns_pk) > 0 else indexes_pk

        return pk

    def get_ddl(self):
        return {
            "columns": [c.dict() for c in self.columns],
            "indexes": {
                k: v.dict() for k, v in self.indexes.items() if k != "primary_key"
            },
            "permissions": self.permissions,
            "primary_key": self.primary_key,
        }


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

    ddl_validation_class = DDL
    sql_features = list()
    # Supported sql_features
    #   - CREATE IF NOT EXISTS
    #   - CREATE TABLE NO PARENTHESES
    #   - INSERT TABLE NO PARENTHESES
    #   - DROP CASCADE
    #   - NO SET SCHEMA
    #   - NO ALTER INDEXES

    _requested_objects = None

    def _request_object(self, name, schema=None, tmp_schema=None, task_name=None):
        to_request = ((name, schema), (f"sayn_tmp_{name}", tmp_schema or schema))

        for name, schema in to_request:
            if schema not in self._requested_objects:
                self._requested_objects[schema] = {name: {"tasks": list()}}
            else:
                self._requested_objects[schema][name] = {"tasks": list()}

            if task_name is not None:
                self._requested_objects[schema][name]["tasks"].append(task_name)

    def _instrospect(self):
        insp = reflection.Inspector.from_engine(self.engine)

        for schema in self._requested_objects.keys():
            for table in insp.get_table_names(schema=schema):
                if table in self._requested_objects[schema]:
                    self._requested_objects[schema][table]["type"] = "table"
            for view in insp.get_view_names(schema=schema):
                if view in self._requested_objects[schema]:
                    self._requested_objects[schema][view]["type"] = "view"

    def __init__(self, name, name_in_settings, db_type, common_params):
        self.name = name
        self.name_in_settings = name_in_settings
        self.db_type = db_type
        self.max_batch_rows = common_params.get("max_batch_rows", 50000)
        self._requested_objects = dict()
        self._jinja_env = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

    def _set_engine(self, engine):
        self.engine = engine
        self.metadata = MetaData(self.engine)

        # Force a query to test the connection
        engine.execute("select 1")

    def _validate_ddl(self, ddl):
        if ddl is None:
            return Ok(self.ddl_validation_class().get_ddl())
        else:
            try:
                return Ok(self.ddl_validation_class(**ddl).get_ddl())
            except Exception as e:
                return Exc(e, db=self.name, type=self.db_type)

    def _transform_column_type(self, column_type, dialect):
        return self._py2sqa(column_type.python_type, dialect=dialect)

    def _refresh_metadata(self, only=None, schema=None):
        """Refreshes the sqlalchemy metadata object.

        Args:
            only (list): A list of object names to filter the refresh on
            schema (str): The schema name to filter on the refresh
        """
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    def _py2sqa(self, from_type, dialect=None):
        python_types = {
            int: sqltypes.BigInteger,
            str: sqltypes.Unicode,
            float: sqltypes.Float,
            decimal.Decimal: sqltypes.Numeric,
            datetime.datetime: sqltypes.DateTime,
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
        elif dialect is not None:
            return python_types[from_type]().compile(dialect=dialect)
        else:
            return python_types[from_type]

    # API

    def execute(self, script):
        """Executes a script in the database. Multiple statements are supported.

        Args:
            script (sql): The SQL script to execute
        """
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(script)

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

        return [dict(zip(res.keys(), r)) for r in res.fetchall()]

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

            for record in res:
                yield dict(zip(res.keys(), record))

    def _load_data_batch(self, table, data, schema):
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
        self, table, data, schema=None, batch_size=None, replace=False, ddl=None
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
        """
        batch_size = batch_size or self.max_batch_rows
        buffer = list()
        if replace:
            self._drop_table(table, schema, execute=True)

        result = self._validate_ddl(ddl)
        if result.is_err:
            raise DBError(
                self.name,
                self.db_type,
                "Incorrect ddl provided",
                errors=result.error["errors"],
            )
        else:
            ddl = result.value

        check_create = True
        table_exists_prior_load = self._table_exists(table, schema)

        for i, record in enumerate(data):
            if check_create and not table_exists_prior_load:
                # Create the table if required
                if len(ddl.get("columns", list())) == 0:
                    # If no columns are specified in the ddl, figure that out
                    # based on the python types of the first record
                    columns = [
                        {
                            "name": col,
                            "type": self._py2sqa(type(val), self.engine.dialect),
                        }
                        for col, val in record.items()
                    ]
                    ddl = dict(ddl, columns=columns)

                self._create_table_ddl(table, schema, ddl, execute=True)
                check_create = False

            if i % batch_size == 0 and len(buffer) > 0:
                self._load_data_batch(table, buffer, schema)
                buffer = list()

            buffer.append(record)

        if len(buffer) > 0:
            self._load_data_batch(table, buffer, schema)

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

    def create_table(
        self, table, schema=None, select=None, replace=False, **ddl,
    ):
        table_name = f"{schema+'.' if schema is not None else ''}{table}"
        object_type = self._requested_objects[schema][table].get("type")
        table_exists = object_type == "table"
        view_exists = object_type == "view"

        template = self._jinja_env.get_template("create_table.sql")
        create = template.render(
            table_name=table_name,
            view_exists=view_exists,
            table_exists=table_exists,
            select=select,
            replace=True,
            can_replace_table="CAN REPLACE TABLE" in self.sql_features,
            **ddl,
        )
        return {"Create Table": create}

    def replace_table(
        self, table, select, schema=None, tmp_schema=None, **ddl,
    ):
        # Create the temporary table
        can_replace_table = "CAN REPLACE TABLE" in self.sql_features

        tmp_table = f"sayn_tmp_{table}"
        tmp_schema = tmp_schema or schema

        if can_replace_table:
            create_or_replace = self.create_table(
                table, schema, select=select, replace=True
            )
            return create_or_replace

        else:
            create_or_replace = self.create_table(
                tmp_table, tmp_schema, select=select, replace=True
            )

            # Move the table to its final location
            template = self._jinja_env.get_template("move_table.sql")

            move = template.render(
                src_schema=tmp_schema or schema,
                src_table=tmp_table,
                dst_schema=schema,
                dst_table=table,
                can_alter_indexes="CAN ALTER INDEXES" in self._sql_features,
                **ddl,
            )

            return dict(create_or_replace, **{"Move table": move})

    def replace_view(self, view, select, schema=None, **ddl):
        view_name = f"{schema+'.' if schema is not None else ''}{view}"
        object_type = self._requested_objects[schema][view].get("type")
        table_exists = object_type == "table"
        view_exists = object_type == "view"

        template = self._jinja_env.get_template("create_view.sql")
        create = template.render(
            table_name=view_name,
            view_exists=view_exists,
            table_exists=table_exists,
            select=select,
            replace=True,
            can_replace_table="CAN REPLACE TABLE" in self.sql_features,
            **ddl,
        )
        return {"Create View": create}

    def merge_query(
        self, table, select, delete_key, schema=None, tmp_schema=None, **ddl
    ):
        tmp_table = f"sayn_tmp_{table}"
        tmp_schema = tmp_schema or schema
        src_table = f"{tmp_schema+'.' if tmp_schema is not None else ''}{tmp_table}"
        dst_table = f"{schema+'.' if schema is not None else ''}{table}"

        create_or_replace = self.create_table(
            tmp_table, tmp_schema, select=select, replace=True
        )

        template = self._jinja_env.get_template("merge_tables.sql")
        merge = template.render(
            dst_table=dst_table, src_table=src_table, delete_key=delete_key
        )

        return dict(create_or_replace, **{"Merge Tables": merge})
