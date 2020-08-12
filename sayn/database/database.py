from itertools import groupby

from sqlalchemy import MetaData, Table

from ..core.errors import DatabaseError
from ..utils import yaml


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
            connection.execute(script)

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

    def validate_ddl(self, ddl, **kwargs):
        if "columns" not in ddl:
            columns = None
        else:
            columns = self.validate_columns(ddl.get("columns"), **kwargs)
            if columns is None:
                return

        if "indexes" not in ddl:
            indexes = None
        else:
            indexes = self.validate_indexes(ddl.get("indexes"), **kwargs)
            if indexes is None:
                return

        if "permissions" not in ddl:
            permissions = None
        else:
            permissions = self.validate_permissions(ddl.get("permissions"), **kwargs)
            if permissions is None:
                return

        return {
            "columns": columns,
            "indexes": indexes,
            "permissions": permissions,
        }

    def validate_columns(self, columns, **kwargs):
        """Validates the columns definition for a task.

        A column definition can be in the formats:
         * str: indicates the column name
         * dict: specificies name, type and other properties supported by the database driver

        Args:
            columns (list): A list of column definitions

        Returns:
            list: A list of dictionaries with the column definition in a dict format or None if
            there was an error during validation
        """
        try:
            ddl = yaml.as_document(
                columns,
                schema=yaml.Seq(
                    yaml.NotEmptyStr()
                    | yaml.Map(
                        {
                            "name": yaml.NotEmptyStr(),
                            yaml.Optional("type"): yaml.NotEmptyStr(),
                            yaml.Optional("primary"): yaml.Bool(),
                            yaml.Optional("not_null"): yaml.Bool(),
                            yaml.Optional("unique"): yaml.Bool(),
                        }
                    )
                ),
            )
        except Exception as e:
            raise DatabaseError(f"{e}")

        ddl = [c if isinstance(c, dict) else {"name": c} for c in ddl.data]

        duplicate_cols = [
            k for k, v in groupby(sorted([c["name"] for c in ddl])) if len(list(v)) > 1
        ]
        if len(duplicate_cols) > 0:
            raise DatabaseError(f"Duplicate columns found: {', '.join(duplicate_cols)}")

        if kwargs.get("types_required"):
            missing_type = [c["name"] for c in ddl if "type" not in c]
            if len(missing_type) > 0:
                raise DatabaseError(
                    f"Missing type for columns: {', '.join(missing_type)}"
                )

        return ddl

    def validate_indexes(self, indexes, **kwargs):
        """Validates the indexes definition for a task.

        Args:
            indexes (dict): A dictionary of indexes with the column list

        Returns:
            list: A dictionary with the index definition or None if there was
            an error during validation
        """
        try:
            ddl = yaml.as_document(
                indexes,
                schema=yaml.MapPattern(
                    yaml.NotEmptyStr(),
                    yaml.Map({"columns": yaml.UniqueSeq(yaml.NotEmptyStr())}),
                ),
            )
        except Exception as e:
            raise DatabaseError(f"{e}")

        return ddl.data

    def validate_permissions(self, permissions, **kwargs):
        """Validates the permissions definition for a task.

        Args:
            permissions (dict): A dictionary in the role -> grant format

        Returns:
            list: A dictionary with the grant list or None if there was an error during validation
        """
        try:
            ddl = yaml.as_document(
                permissions,
                schema=yaml.MapPattern(
                    yaml.NotEmptyStr(),
                    yaml.NotEmptyStr() | yaml.UniqueSeq(yaml.NotEmptyStr()),
                ),
            )
        except Exception as e:
            raise DatabaseError(f"{e}")

        return ddl.data

    def refresh_metadata(self, only=None, schema=None):
        """Refreshes the sqlalchemy metadata object.

        Args:
            only (list): A list of object names to filter the refresh on
            schema (str): The schema name to filter on the refresh
        """
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    def get_table(self, table, schema, columns=None, required_existing=False):
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
        table_def = Table(table, self.metadata, schema=schema, extend_existing=True)

        if table_def.exists():
            self.refresh_metadata(only=[table], schema=schema)
            if columns is not None:
                cols_in_table = set([c.name for c in table_def.columns])
                cols_requested = set(
                    [c.name if not isinstance(c, str) else c for c in columns]
                )

                if len(cols_requested - cols_in_table) > 0:
                    raise DatabaseError(
                        f"Missing columns \"{', '.join(cols_requested - cols_in_table)}\" in table \"{table_def.name}\""
                    )

                if len(cols_in_table - cols_requested) > 0:
                    for column in cols_in_table - cols_requested:
                        table_def._columns.remove(table_def.columns[column])
        elif required_existing:
            return
        elif columns is not None:
            for column in columns:
                table_def.append_column(column.copy())

        return table_def

    def table_exists(self, table, schema, with_columns=None):
        table_def = self.get_table(table, schema, columns=with_columns)
        if table_def is not None:
            return table_def.exists()
        else:
            return False

    # ETL steps return SQL code ready for execution

    def create_table_select(
        self, table, schema, select, replace=False, view=False, ddl=dict()
    ):
        """Returns SQL code for a create table from a select statment.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): A SQL SELECT query to build the table with
            replace (bool): Issue a DROP statement
            view (bool): Indicates if the object to create is a view. Defaults to creating a table

        Returns:
            str: A SQL script for the CREATE...AS
        """
        table_name = table
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if replace:
            q += self.drop_table(table_name, schema, view) + "\n"
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        if "CREATE TABLE NO PARENTHESES" in self.sql_features:
            q += f"CREATE {table_or_view}{if_not_exists} {table} AS \n{select}\n;"
        else:
            q += f"CREATE {table_or_view}{if_not_exists} {table} AS (\n{select}\n);"

        return q

    def create_table_ddl(self, table, schema, ddl, replace=False):
        """Returns SQL code for a create table from a select statment.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition
            replace (bool): Issue a DROP statement

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
        if replace:
            q += self.drop_table(table_name, schema) + "\n"
        if_not_exists = (
            " IF NOT EXISTS" if "CREATE IF NOT EXISTS" in self.sql_features else ""
        )
        q += f"CREATE TABLE{if_not_exists} {table} (\n      {columns}\n);"

        return q

    def create_indexes(self, table, schema, ddl):
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

        return q

    def grant_permissions(self, table, schema, ddl):
        """Returns a set of GRANT statments.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            ddl (dict): A ddl task definition

        Returns:
            str: A SQL script for the GRANT statements
        """
        return "\n".join(
            [
                f"GRANT {priv} ON {schema+'.' if schema else ''}{table} TO \"{role}\";"
                for role, priv in ddl.items()
            ]
        )

    def drop_table(self, table, schema, view=False):
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

        return q

    def insert(self, table, schema, select):
        """Returns an INSERT statment from a SELECT query.

        Args:
            table (str): The target table name
            schema (str): The target schema or None
            select (str): The SELECT statement to issue

        Returns:
            str: A SQL script for the INSERT statement
        """
        table = f"{schema+'.' if schema else ''}{table}"
        return f"INSERT INTO {table} (\n{select}\n);"

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
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
        drop = self.drop_table(dst_table, dst_schema)
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

        return "\n".join([drop, rename, change_schema] + idx_alter)

    def merge_tables(self, src_table, src_schema, dst_table, dst_schema, delete_key):
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
        insert = f"INSERT INTO {dst} SELECT * FROM {src};"
        drop = self.drop_table(src_table, src_schema)
        return "\n".join((delete, insert, drop))
