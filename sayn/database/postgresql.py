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
            q += f"DROP {table_or_view} IF EXISTS {table};\n"
        q += f"CREATE {table_or_view} {table} AS (\n{select}\n);"

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
        q += f"CREATE TABLE {table} (\n      {columns}\n);\n"
        q += "\n".join(
            [
                f"CREATE INDEX {name} ON {table}({', '.join(cols)});"
                for name, cols in indexes.items()
            ]
        )

        return q
