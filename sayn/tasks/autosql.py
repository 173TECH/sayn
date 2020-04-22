import logging

from .sql import SqlTask
from .task import TaskStatus


class AutoSqlTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_task_def()
        if status != TaskStatus.READY:
            return status

        status = self._setup_ddl()
        if status != TaskStatus.READY:
            return status

        status = self._setup_sql()
        if status != TaskStatus.READY:
            return status

        return self.ready()

    def _setup_task_def(self):
        # Process autosql config
        self.materialisation = self._task_def.get("materialisation", "table")
        if self.materialisation not in ("view", "table", "incremental"):
            return self.failed(
                (
                    f'"{self.materialisation}" is not a correct materialisation.'
                    ' Valid values are: "table", "incremental" or "view"'
                )
            )

        if self.materialisation == "incremental":
            if "delete_key" not in self._task_def:
                return self.failed(
                    '"delete_key" is required for incremental autosql tasks'
                )
            else:
                self.delete_key = self._task_def["delete_key"]

        if "to" not in self._task_def:
            return self.failed('Missing "to" field')

        self.staging_schema = self._task_def["to"].get("staging_schema")
        if self.staging_schema is not None:
            self.staging_schema = self.compile_property(self.staging_schema)

        self.schema = self._task_def["to"].get("schema")
        if self.schema is not None:
            self.schema = self.compile_property(self.schema)

        if "table" not in self._task_def["to"]:
            return self.failed('Missing "table" field in "to"')
        else:
            self.table = self.compile_property(self._task_def["to"]["table"])
            self.staging_table = "sayn_tmp_" + self.table

        return TaskStatus.READY

    def _setup_sql(self):
        # Get the base query
        self.template = self.get_query()

        if self.template is None:
            return self.failed()

        try:
            query = self.template.render(**self.parameters)
        except Exception as e:
            return self.failed(f"Error compiling template\n{e}")

        if self.ddl is not None and "permissions" in self.ddl:
            permissions = "\n\n-- Grant permissions\n" + self.db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"]
            )
        else:
            permissions = ""

        if self.materialisation == "view":
            # Views just replace the current object if it exists
            self.compiled = (
                self.db.create_table_select(
                    self.table, self.schema, query, replace=True, view=True,
                )
                + permissions
            )
            return self.ready()

        # Some common statements
        staging_table = self.db.create_table_select(
            self.staging_table, self.staging_schema, query, replace=True,
        )

        move = (
            self.db.move_table(
                self.staging_table,
                self.staging_schema,
                self.table,
                self.schema,
                self.ddl,
            )
            + permissions
        )

        if not self.db.table_exists(self.table, self.schema):
            # When the table doesn't currently exists, regardless of materialisation, just create it
            if self.ddl is not None and "columns" in self.ddl:
                # create table with DDL and insert the output of the select
                create_table_ddl = (
                    self.db.create_table_ddl(
                        self.table,
                        self.schema,
                        self.ddl,
                        replace=self.sayn_config.options["full_load"],
                    )
                    + permissions
                )
                insert = self.db.insert(self.table, self.schema, query)
                self.compiled = (
                    f"-- Create table\n"
                    f"{create_table_ddl}\n\n"
                    f"-- Load data\n"
                    f"{insert}\n\n"
                )

            else:
                # or create from the select if DDL not present
                self.compiled = (
                    self.db.create_table_select(
                        self.table,
                        self.schema,
                        query,
                        replace=self.sayn_config.options["full_load"],
                    )
                    + permissions
                )

        # If the table exists and there's DDL defined, ...
        elif (
            self.materialisation == "table"
            and self.ddl is not None
            and "columns" in self.ddl
        ):
            # ... when it's a TABLE materialisation: create, load and replace table with temporary
            create_staging_ddl = self.db.create_table_ddl(
                self.staging_table, self.staging_schema, self.ddl, replace=True,
            )
            insert_staging = self.db.insert(
                self.staging_table, self.staging_schema, query
            )
            self.compiled = (
                f"-- Create temporary table\n"
                f"{create_staging_ddl}\n\n"
                f"-- Load data\n"
                f"{insert_staging}\n\n"
                f"-- Move data to final table\n"
                f"{move}"
            )

        elif self.materialisation == "table" and (
            self.ddl is None or "columns" not in self.ddl
        ):
            # ... when it exists and it's a TABLE materialisation, create in temp and move
            self.compiled = (
                f"-- Create temporary table\n"
                f"{staging_table}\n\n"
                f"-- Move data to final table\n"
                f"{move}"
            )

        elif self.materialisation == "incremental":
            # ... whether there's DDL or not, ...
            # ... when it's a INCREMENTAL materialisation: MERGE instead of load
            merge = self.db.merge_tables(
                self.staging_table,
                self.staging_schema,
                self.table,
                self.schema,
                self.delete_key,
            )
            self.compiled = (
                f"-- Create temporary table\n"
                f"{staging_table}\n\n"
                f"-- Move data to final table\n"
                f"{merge}\n\n"
            )

        else:
            raise ValueError("SAYN issue")

        return TaskStatus.READY
