from collections import Counter
import datetime
import decimal
from typing import Dict, List, Optional

from pydantic import BaseModel, validator, conlist
from sqlalchemy import MetaData, Table
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
    primary_key: Optional[
        List[str]
    ] = []  # logic field - i.e. not added by the user in the ddl definition
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
    sql_features = []
    # Supported sql_features
    #   - CREATE IF NOT EXISTS
    #   - CREATE TABLE NO PARENTHESES
    #   - INSERT TABLE NO PARENTHESES
    #   - DROP CASCADE
    #   - NO SET SCHEMA
    #   - NO ALTER INDEXES

    def __init__(self, name, name_in_settings, db_type, common_params):
        self.name = name
        self.name_in_settings = name_in_settings
        self.db_type = db_type
        self.max_batch_rows = common_params.get("max_batch_rows", 50000)

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

    # ETL steps
    # =========
    # Methods that build sql used in autosql and
    # copy tasks. Can optionally also execute the
    # sql if `execute=True`

    def _create_table_select(
        self, table, schema, select, view=False, ddl=dict(), execute=False
    ):
        """Returns SQL code for a create table from a select statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): A SQL SELECT query to build the table with
            view (bool): Indicates if the object to create is a view. Defaults to creating a table
            ddl (dict): Optionally specify a ddl dict. If provided, a `CREATE` with column specification
                followed by an `INSERT` rather than a `CREATE ... AS SELECT ...` will be issued
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the CREATE...AS
        """
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        if "CREATE TABLE NO PARENTHESES" in self.sql_features:
            q += f"CREATE {table_or_view}{if_not_exists} {table} AS \n{select}\n;"
        else:
            q += f"CREATE {table_or_view}{if_not_exists} {table} AS (\n{select}\n);"

        if execute:
            self.execute(q)

        return q

    def _create_table_ddl(self, table, schema, ddl, execute=False):
        """Returns SQL code for a create table from a select statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the CREATE TABLE statement
        """
        if len(ddl["columns"]) == 0:
            raise DBError(
                self.name, self.db_type, "DDL is missing columns specification"
            )
        table_name = table
        table = f"{schema+'.' if schema else ''}{table_name}"

        # List of reserved keywords so columns are quoted
        # TODO find a better way
        reserved = ("from", "to", "primary")
        columns = [
            {k: f'"{v}"' if k == "name" and v in reserved else v for k, v in c.items()}
            for c in ddl["columns"]
        ]

        columns = "\n    , ".join(
            [
                (
                    f'{c["name"]} {c["type"]}'
                    f'{" NOT NULL" if c.get("not_null", False) else ""}'
                )
                for c in columns
            ]
        )

        if len(ddl["primary_key"]) > 0:
            pk = " ,".join(ddl["primary_key"])
            pk = f"    , PRIMARY KEY ({pk})"
        else:
            pk = ""

        q = ""
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        q += f"CREATE TABLE{if_not_exists} {table} (\n      {columns}\n{pk}\n);"

        if execute:
            self.execute(q)

        return q

    def _create_indexes(self, table, schema, ddl, execute=False):
        """Returns SQL to create indexes from ddl.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the CREATE INDEX statements
        """
        table_name = table
        table = f"{schema+'.' if schema else ''}{table}"

        indexes = {
            idx: idx_def["columns"]
            for idx, idx_def in ddl.get("indexes", dict()).items()
        }

        q = ""
        if len(ddl["primary_key"]) > 0:
            pk_cols = ", ".join(ddl["primary_key"])
            q += f"ALTER TABLE {table} ADD PRIMARY KEY ({pk_cols});"

        q += "\n".join(
            [
                f"CREATE INDEX {table_name}_{name} ON {table}({', '.join(cols)});"
                for name, cols in indexes.items()
            ]
        )

        if execute:
            self.execute(q)

        return q

    def grant_permissions(self, table, schema, ddl, execute=False):
        """Returns a set of GRANT statements.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the GRANT statements
        """
        q = "\n".join(
            [
                f"GRANT {priv} ON {schema+'.' if schema else ''}{table} TO \"{role}\";"
                for role, priv in ddl.items()
            ]
        )

        if execute:
            self.execute(q)

        return q

    def _drop_table(self, table, schema, view=False, execute=False):
        """Returns a DROP statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            view (bool): Indicates if the object to drop is a view. Defaults to dropping a table
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the DROP statements
        """
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = f"DROP {table_or_view} IF EXISTS {table}"

        if "DROP CASCADE" in self.sql_features:
            q += " CASCADE;"
        else:
            q += ";"

        if execute:
            self.execute(q)

        return q

    def _insert(self, table, schema, select, columns=None, execute=False):
        """Returns an INSERT statement from a SELECT query.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): The SELECT statement to issue
            columns (list): The list of column names specified in DDL. If provided, the insert will be reordered based on this order
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for the INSERT statement
        """
        table = f"{schema+'.' if schema else ''}{table}"

        # we reshape the insert statement to avoid conflict if columns are not specified in same order between query and task group file
        if columns is not None:
            select = "SELECT i." + "\n, i.".join(columns) + f"\n\nFROM ({select}) AS i"
            columns = "(" + ", ".join(columns) + ")"
        else:
            columns = ""

        if "INSERT TABLE NO PARENTHESES" in self.sql_features:
            q = f"INSERT INTO {table} {columns} \n{select}\n;"
        else:
            q = f"INSERT INTO {table} {columns} (\n{select}\n);"

        if execute:
            self.execute(q)

        return q

    def _move_table(
        self, src_table, src_schema, dst_table, dst_schema, ddl, execute=False
    ):
        """Returns SQL code to rename a table and change schema.

        Note:
            Table movement is performed as a series of ALTER statements:

              * ALTER TABLE RENAME
              * ALTER TABLE SET SCHEMA (if the database supports it)
              * ALTER INDEX RENAME (to ensure consistency in the naming). Index names
                  are taken from the ddl field

        Args:
            src_table (str): The source table name
            src_schema (str): The source schema or None
            dst_table (str): The target table name
            dst_schema (str): The target schema or None
            ddl (dict): A ddl task definition
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for moving the table
        """
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_table};"
        if dst_schema is not None and dst_schema != src_schema:
            change_schema = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{dst_table} SET SCHEMA {dst_schema};"
        else:
            change_schema = ""

        pk_alter = []
        if "NO ALTER INDEXES" not in self.sql_features and len(ddl["primary_key"]) > 0:
            # Change primary key name
            pk_alter.append(
                f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"
            )

        idx_alter = []
        if len(ddl["indexes"]) > 0:
            # Change index names
            for idx in ddl["indexes"].keys():
                if "NO ALTER INDEXES" in self.sql_features:
                    idx_cols = " ,".join(ddl["indexes"][idx]["columns"])
                    idx_alter.append(
                        f"DROP INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_{idx};\n"
                        f"CREATE INDEX {dst_table}_{idx} ON {dst_table}({idx_cols});"
                    )
                else:
                    idx_alter.append(
                        f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_{idx} "
                        f"RENAME TO {dst_table}_{idx};"
                    )

        q = "\n".join([rename, change_schema] + pk_alter + idx_alter)

        if execute:
            self.execute(q)

        return q

    def _merge_tables(
        self,
        src_table,
        src_schema,
        dst_table,
        dst_schema,
        delete_key,
        columns=None,
        execute=False,
    ):
        """Returns SQL to merge data in incremental loads.

        Note:
            Data merge is performed by issuing these statements:

              * DELETE from target WHERE data exists in source
              * INSERT into target SELECT * from source

        Args:
            src_table (str): The source table name
            src_schema (str): The source schema or None
            dst_table (str): The target table name
            dst_schema (str): The target schema or None
            delete_key (str): The column name to use for deleting records from the target table
            columns (list): The list of column names specified in DDL. If provided, the insert will be reordered based on this order
            execute (bool): Execute the query before returning it

        Returns:
            str: A SQL script for moving the table
        """
        dst = f"{dst_schema+'.' if dst_schema else ''}{dst_table}"
        src = f"{src_schema+'.' if src_schema else ''}{src_table}"

        delete = (
            f"DELETE FROM {dst}\n"
            f" WHERE EXISTS (SELECT *\n"
            f"                 FROM {src}\n"
            f"                WHERE {src}.{delete_key} = {dst}.{delete_key});"
        )

        select = f"SELECT * FROM {src}"
        insert = self._insert(dst_table, dst_schema, select, columns=columns)
        q = "\n".join((delete, insert))

        if execute:
            self.execute(q)

        return q
