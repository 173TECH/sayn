from collections import Counter
from typing import Dict, List, Optional

from pydantic import BaseModel, validator
from sqlalchemy import MetaData, Table

from ..core.errors import Err, Exc, Ok

from ..utils.misc import group_list


class DDL(BaseModel):
    class Column(BaseModel):
        name: str
        type: str
        primary: Optional[bool]
        not_null: Optional[bool]
        unique: Optional[bool]

    class Index(BaseModel):
        columns: List[str]

    columns: Optional[List[Column]]
    indexes: Dict[str, Index]
    permissions: Dict[str, str]

    @validator("columns")
    def columns_unique(cls, v, values):
        print(v)
        dupes = {k for k, v in Counter([e.name for e in v]) if v > 1}
        if len(dupes) > 0:
            raise ValueError(f"Duplicate columns: {','.join(dupes)}")
        else:
            return v

    @validator("indexes")
    def index_columns_exists(cls, v, values):
        cols = [c.name for c in values["columns"]]
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


class Database:
    """
    Base class for databases in SAYN.

    Databases are implemented using sqlalchemy, and the `engine` attribute is available
    when working in python tasks without the need for calling create_engine.

    Attributes:
        * engine (sqlalchemy.Engine): A sqlalchemy engine referencing the database
        * name (str): Name of the db as defined in `required_credentials` in `project.yaml`
        * name_in_yaml (str): Name of db under `credentials` in `settings.yaml`
        * db_type (str): Type of the database
    """

    sql_features = []
    # Supported sql_features
    #   - CREATE IF NOT EXISTS
    #   - CREATE TABLE NO PARENTHESES
    #   - INSERT TABLE NO PARENTHESES
    #   - DROP CASCADE
    #   - NO SET SCHEMA

    def setup_db(self, name, name_in_settings, db_type, engine):
        self.name = name
        self.name_in_settings = name_in_settings
        self.db_type = db_type
        self.engine = engine
        self.metadata = MetaData(self.engine)

    # API

    def execute(self, script):
        """Executes a script in the database. Multiple statements are supported.

        Args:
            script (sql): The SQL script to execute
        """
        with self.engine.connect().execution_options(autocommit=True) as connection:
            try:
                result = Ok(connection.execute(script))
            except Exception as e:
                result = Err(
                    "database_error",
                    "sql_execution_error",
                    {"exception": e, "db": self.name, "script": script},
                )
            finally:
                return result

    def select(self, query, **params):
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

    def select_stream(self, query, **params):
        """Executes the query and returns an iterator dictionaries with the data.

        The main difference with select() is that this method executes the query with a server-side
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

            for record in res.cursor:
                yield dict(zip(res.keys(), record))

    def load_data(self, table, schema, data):
        """Loads a list of values into the database

        The default loading mechanism is an INSERT...VALUES, but database drivers
        will implement more appropriate methods.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            data (list): A list of dictionaries to load
        """
        table_def = self.get_table(table, schema)
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(table_def.insert().values(data))

    # DDL validation methods

    def validate_ddl(self, ddl):
        default_dict = {"columns": list(), "indexes": dict(), "permissions": dict()}
        if ddl is None:
            return Ok(default_dict)
        else:
            return Ok(dict(**default_dict, **DDL(**ddl).dict()))

    def refresh_metadata(self, only=None, schema=None):
        """Refreshes the sqlalchemy metadata object.

        Args:
            only (list): A list of object names to filter the refresh on
            schema (str): The schema name to filter on the refresh
        """
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    def get_table(self, table, schema):
        """Create a SQLAlchemy Table object.

        Args:
            table (str): The table name
            schema (str): The schema or None
            columns (list): A list of column names to build the table object
            required_existing (bool): If True and columns is not None, fills up
                the table columns with the specification in columns

        Returns:
            sqlalchemy.Table: A table object from sqlalchemy
        """
        # TODO control exceptions
        table_def = Table(table, self.metadata, schema=schema, extend_existing=True)

        if table_def.exists():
            self.refresh_metadata(only=[table], schema=schema)
            return Ok(table_def)
        else:
            return Err(
                "database_error",
                "table_not_exists",
                {"db": self.name, "table": table, "schema": schema},
            )

    def table_exists(self, table, schema, with_columns=None):
        table_def = self.get_table(table, schema, columns=with_columns)
        if table_def is not None:
            return table_def.exists()
        else:
            return False

    # ETL steps return SQL code ready for execution

    def create_table_select(
        self, table, schema, select, view=False, ddl=dict(), execute=False
    ):
        """Returns SQL code for a create table from a select statment.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): A SQL SELECT query to build the table with
            view (bool): Indicates if the object to create is a view. Defaults to creating a table

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

        return Ok(q)

    def create_table_ddl(self, table, schema, ddl, execute=False):
        """Returns SQL code for a create table from a select statment.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition

        Returns:
            str: A SQL script for the CREATE TABLE statement
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
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        q += f"CREATE TABLE{if_not_exists} {table} (\n      {columns}\n);"

        if execute:
            self.execute(q)

        return Ok(q)

    def create_indexes(self, table, schema, ddl, execute=False):
        """Returns SQL to create indexes from ddl.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition

        Returns:
            str: A SQL script for the CREATE INDEX statements
        """
        table_name = table
        table = f"{schema+'.' if schema else ''}{table}"

        indexes = {
            idx: idx_def["columns"]
            for idx, idx_def in ddl.get("indexes", dict()).items()
            if idx != "primary_key"
        }

        q = ""
        if "primary_key" in ddl.get("indexes", dict()):
            pk_cols = ", ".join(ddl["indexes"]["primary_key"]["columns"])
            q += f"ALTER TABLE {table} ADD PRIMARY KEY ({pk_cols});"

        q += "\n".join(
            [
                f"CREATE INDEX {table_name}_{name} ON {table}({', '.join(cols)});"
                for name, cols in indexes.items()
            ]
        )

        if execute:
            self.execute(q)

        return Ok(q)

    def grant_permissions(self, table, schema, ddl, execute=False):
        """Returns a set of GRANT statments.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition

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

    def drop_table(self, table, schema, view=False, execute=False):
        """Returns a DROP statement.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            view (bool): Indicates if the object to drop is a view. Defaults to dropping a table

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

        return Ok(q)

    def insert(self, table, schema, select, columns=None, execute=False):
        """Returns an INSERT statment from a SELECT query.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): The SELECT statement to issue
            columns (list): The list of column names specified in DDL. If provided, the insert will be reordered based on this order

        Returns:
            str: A SQL script for the INSERT statement
        """
        table = f"{schema+'.' if schema else ''}{table}"

        # we reshape the insert statement to avoid conflict if columns are not specified in same order between query and dag file
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

        return Ok(q)

    def move_table(
        self, src_table, src_schema, dst_table, dst_schema, ddl, execute=False
    ):
        """Returns SQL code to rename a table and change schema.

        Note:
            Table movement is performed as a series of ALTER statments:

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

        Returns:
            str: A SQL script for moving the table
        """
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_table};"
        if dst_schema is not None and dst_schema != src_schema:
            change_schema = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{dst_table} SET SCHEMA {dst_schema};"
        else:
            change_schema = ""

        idx_alter = []
        if ddl.get("indexes") is not None:
            # Change index names
            for idx in ddl["indexes"].keys():
                if idx == "primary_key":
                    # Primary keys are called as the table
                    idx_alter.append(
                        f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"
                    )
                else:
                    idx_alter.append(
                        f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_{idx} RENAME TO {dst_table}_{idx};"
                    )

        q = "\n".join([rename, change_schema] + idx_alter)

        if execute:
            self.execute(q)

        return Ok(q)

    def merge_tables(
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
        insert = self.insert(dst_table, dst_schema, select, columns=columns)
        q = "\n".join((delete, insert))

        if execute:
            self.execute(q)

        return Ok(q)
