from itertools import groupby

from .database import Database


class Postgresql(Database):
    def __init__(self, name, name_in_settings, settings):
        self.dialect = "postgresql"
        connection_details = settings
        connection_details.pop("type")
        super().__init__(name, name_in_settings, connection_details)

    def create_table_select(self, table, schema, select, replace=False, view=False):
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"

        q = ""
        if replace:
            q += f"DROP {table_or_view} IF EXISTS {table} CASCADE;\n"
        q += f"CREATE {table_or_view} {table} AS (\n{select}\n);"

        return q

    def create_table_ddl(self, table, schema, ddl, replace=False):
        table_name = table
        table = f"{schema+'.' if schema else ''}{table_name}"
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
            q += f"DROP TABLE IF EXISTS {table} CASCADE;\n"
        q += f"CREATE TABLE {table} (\n      {columns}\n);\n"
        q += "\n".join(
            [
                f"CREATE INDEX {table_name}_{name} ON {table}({', '.join(cols)});"
                for name, cols in indexes.items()
            ]
        )

        return q

    def drop_table(self, table, schema, view=False):
        table = f"{schema+'.' if schema else ''}{table}"
        table_or_view = "VIEW" if view else "TABLE"
        return f"DROP {table_or_view} IF EXISTS {table} CASCADE;"

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table} CASCADE;"
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_table};"
        if dst_schema is not None:
            change_schema = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{dst_table} SET SCHEMA {dst_schema};"
        else:
            change_schema = ""

        idx_alter = []
        if ddl is not None and "columns" in ddl:
            # Change primary key name
            if len([c for c in ddl["columns"] if c.get("primary", False)]):
                idx_alter.append(
                    f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"
                )

            # Change index names
            for idx in [
                idx for c in ddl["columns"] for idx in c.get("indexes", list())
            ]:
                idx_alter.append(
                    f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_{idx} RENAME TO {dst_table}_{idx};"
                )

        return "\n".join([drop, rename, change_schema] + idx_alter)
