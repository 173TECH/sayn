from itertools import groupby
import logging

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select, func, or_, text

from . import DatabaseError
from ..utils import yaml


class Database:
    def __init__(self, name, name_in_yaml, connection_details):
        self.name = name
        self.name_in_yaml = name_in_yaml
        self.engine = create_engine(f"{self.dialect}://", **connection_details)
        self.create_metadata()

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
                            yaml.Optional("indexes"): yaml.UniqueSeq(
                                yaml.NotEmptyStr()
                            ),
                        }
                    )
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

    def connect(self):
        return self.engine.connect()

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(script)

    def select(self, query, params=None):
        if params is not None:
            res = self.engine.execute(query, **params)
        else:
            res = self.engine.execute(query)

        return [dict(zip(res.keys(), r)) for r in res.fetchall()]

    def load_data(self, table, schema, data):
        table_def = self.get_table(table, schema)
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(table_def.insert().values(data))

    def create_metadata(self):
        self.metadata = MetaData(self.engine)

    def refresh_metadata(self, only=None, schema=None):
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    # ETL steps

    def get_table(self, table, schema, columns=None):
        """Create a SQLAlchemy Table object. If columns is not None, fills up columns or checks the columns are present"""
        table_def = Table(table, self.metadata, schema=schema, extend_existing=True)

        if table_def.exists():
            self.refresh_metadata(only=[table], schema=schema)
            if columns is not None:
                cols_in_table = set([c.name for c in table_def.columns])
                cols_requested = set(
                    [c.name if not isinstance(c, str) else c for c in columns]
                )

                if len(cols_requested - cols_in_table) > 0:
                    logging.error(
                        f"Missing columns \"{', '.join(cols_requested - cols_in_table)}\" in table \"{table_def.name}\""
                    )
                    return

                if len(cols_in_table - cols_requested) > 0:
                    for column in cols_in_table - cols_requested:
                        table_def._columns.remove(table_def.columns[column])

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

    def create_table_select(self, table, schema, select, replace=False, view=False):
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if replace:
            q += f"DROP {table_or_view} IF EXISTS {table};\n"
        q += f"CREATE {table_or_view} IF NOT EXISTS {table} AS (\n{select}\n);"

        return q

    def create_table_ddl(self, table, schema, ddl, replace=False):
        table = f"{schema+'.' if schema else ''}{table}"
        columns = "\n    , ".join(
            [
                (
                    f'{c["name"]} {c["type"]}'
                    f'{" PRIMARY KEY" if c.get("primary", False) else ""}'
                    f'{" NOT NULL" if c.get("not_null", False) else ""}'
                )
                for c in ddl["columns"]
            ]
        )
        indexes = {
            idx: [c[1] for c in cols]
            for idx, cols in groupby(
                sorted(
                    [
                        (idx, c["name"])
                        for c in ddl["columns"]
                        if "indexes" in c
                        for idx in c["indexes"]
                    ]
                ),
                lambda x: x[0],
            )
        }

        q = ""
        if replace:
            q += f"DROP TABLE IF EXISTS {table};\n"
        q += f"CREATE TABLE IF NOT EXISTS {table} (\n      {columns}\n);\n"
        q += "\n".join(
            [
                f"CREATE INDEX IF NOT EXISTS {name} ON {table}({', '.join(cols)});"
                for name, cols in indexes.items()
            ]
        )

        return q

    def grant_permissions(self, table, schema, ddl):
        return "\n".join(
            [
                f"GRANT {priv} ON {schema+'.' if schema else ''}{table} TO \"{role}\";"
                for role, priv in ddl.items()
            ]
        )

    def drop_table(self, table, schema, view=False):
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"
        return f"DROP {table_or_view} IF EXISTS {table};"

    def insert(self, table, schema, select):
        table = f"{schema+'.' if schema else ''}{table}"
        return f"INSERT INTO {table} (\n{select}\n);"

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_table};"
        if dst_schema is not None:
            change_schema = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{dst_table} SET SCHEMA {dst_schema};"
        else:
            change_schema = ""
        pkey_alter = ""
        if ddl is not None and 'columns' in ddl:
            if len([c for c in ddl["columns"] if c.get("primary", False)]):
                pkey_alter = f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"

        return "\n".join((drop, rename, change_schema, pkey_alter))

    def merge_tables(self, src_table, src_schema, dst_table, dst_schema, delete_key):
        dst = f"{dst_schema+'.' if dst_schema else ''}{dst_table}"
        src = f"{src_schema+'.' if src_schema else ''}{src_table}"

        delete = (
            f"DELETE FROM {dst}\n"
            f" WHERE EXISTS (SELECT *\n"
            f"                 FROM {src}\n"
            f"                WHERE {src}.{delete_key} = {dst}.{delete_key});"
        )
        insert = f"INSERT INTO {dst} SELECT * FROM {src};"
        drop = f"DROP TABLE {src};"
        return "\n".join((delete, insert, drop))

    def copy_from_table(
        self, src, dst_table, dst_schema=None, columns=None, incremental_column=None
    ):
        # 1. Check source table
        if not src.exists():
            raise DatabaseError(f"Source table {src.fullname} does not exists")

        if columns is None:
            columns = [c.name for c in src.columns]
            if incremental_column is not None and incremental_column not in src.columns:
                raise DatabaseError(
                    f"Incremental column {incremental_column} not in source table {src.fullname}"
                )
        else:
            if incremental_column is not None and incremental_column not in columns:
                columns.append(incremental_column)

            for column in columns:
                if column not in src.columns:
                    raise DatabaseError(
                        f"Specified column {column} not in source table {src.fullname}"
                    )

        # 2. Get incremental value
        table = self.get_table(dst_table, dst_schema, columns=columns)

        if incremental_column is not None and not dst_table.exists():
            if incremental_column not in table.columns:
                raise DatabaseError(
                    f"Incremental column {incremental_column} not in destination table {table.fullname}"
                )
            incremental_value = (
                select([func.max(table.c[incremental_column])])
                .where(table.c[incremental_column] != None)
                .execute()
                .fetchone()[0]
            )
        elif table is None:
            # Create table
            table = Table(dst_table, self.metadata, schema=dst_schema)
            for column in columns:
                table.append_column(src.columns[column].copy())
            table.create()
            incremental_value = None
        else:
            incremental_value = None

        # 3. Get data
        if incremental_value is None:
            where_cond = True
        else:
            where_cond = or_(
                src.c[incremental_column] == None,
                src.c[incremental_column] > incremental_value,
            )
        query = select([src.c[c] for c in columns]).where(where_cond)
        data = query.execute().fetchall()

        # 4. Load data
        return self.load_data(
            [dict(zip([str(c.name) for c in query.c], row)) for row in data], table
        )

    def get_max_value(self, table, schema, column):
        table_def = self.get_table(table, schema)
        return select([func.max(table_def.c[column])]).where(
            table_def.c[column] != None
        )

    def get_data(self, table, schema, columns, incremental_key=None):
        src = self.get_table(table, schema, columns)
        q = select([src.c[c] for c in columns])
        if incremental_key is not None:
            q = q.where(
                or_(
                    src.c[incremental_key] == None,
                    src.c[incremental_key] > text(":incremental_value"),
                )
            )
        return q
