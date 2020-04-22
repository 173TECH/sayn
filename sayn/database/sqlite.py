from sqlalchemy import create_engine

from .database import Database


class Sqlite(Database):
    def __init__(self, name, name_in_yaml, yaml):
        self.name = name
        self.name_in_yaml = name_in_yaml
        self.dialect = "sqlite"
        self.engine = create_engine(f'sqlite:///{yaml["database"]}')
        self.create_metadata()

    #Sqlite has a different syntax for create table as which does not use ()
    def create_table_select(self, table, schema, select, replace=False, view=False):
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if replace:
            q += f"DROP {table_or_view} IF EXISTS {table};\n"
        q += f"CREATE {table_or_view} IF NOT EXISTS {table} AS \n{select}\n;"

        return q

    #Sqlite does not support ALTER TABLE SET SCHEMA. ALTER for Sqlite can only be used to rename a table
    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_table};"
        pkey_alter = ""
        if ddl is not None and 'columns' in ddl:
            if len([c for c in ddl["columns"] if c.get("primary", False)]):
                pkey_alter = f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"

        return "\n".join((drop, rename, pkey_alter))

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.connection.executescript(script)
