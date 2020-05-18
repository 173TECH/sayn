from .sql import SqlTask
from .task import TaskStatus


class AutoSqlTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_file_name()
        if status != TaskStatus.READY:
            return self.failed()

        self.template = self._get_query_template()
        if self.template is None:
            return self.failed()

        status = self._setup_materialisation()
        if status != TaskStatus.READY:
            return status

        status = self._setup_destination()
        if status != TaskStatus.READY:
            return status

        status = self._setup_ddl()
        if status != TaskStatus.READY:
            return status

        status = self._setup_sql()
        if status != TaskStatus.READY:
            return status

        return self._check_extra_fields()

    # Task property methods

    def _setup_materialisation(self):
        # Type of materialisation
        self.materialisation = self._pop_property("materialisation")
        if self.materialisation is None:
            return self.failed(
                '"materialisation" is a required field (values: table, incremental, view)'
            )
        elif not isinstance(self.materialisation, str) or self.materialisation not in (
            "table",
            "incremental",
            "view",
        ):
            return self.failed(
                'Accepted "materialisation" values: table, incremental, view)'
            )
        elif self.materialisation == "incremental":
            self.delete_key = self._pop_property("delete_key")
            if self.delete_key is None:
                return self.failed("Incremental materialisation requires delete_key")

        return TaskStatus.READY

    def _setup_sql(self):
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
        tmp_table = self.db.create_table_select(
            self.tmp_table, self.tmp_schema, query, replace=True,
        )

        move = (
            self.db.move_table(
                self.tmp_table,
                self.tmp_schema,
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
            create_tmp_ddl = self.db.create_table_ddl(
                self.tmp_table, self.tmp_schema, self.ddl, replace=True,
            )
            insert_tmp = self.db.insert(
                self.tmp_table, self.tmp_schema, query
            )
            self.compiled = (
                f"-- Create temporary table\n"
                f"{create_tmp_ddl}\n\n"
                f"-- Load data\n"
                f"{insert_tmp}\n\n"
                f"-- Move data to final table\n"
                f"{move}"
            )

        elif self.materialisation == "table" and (
            self.ddl is None or "columns" not in self.ddl
        ):
            # ... when it exists and it's a TABLE materialisation, create in temp and move
            self.compiled = (
                f"-- Create temporary table\n"
                f"{tmp_table}\n\n"
                f"-- Move data to final table\n"
                f"{move}"
            )

        elif self.materialisation == "incremental":
            # ... whether there's DDL or not, ...
            # ... when it's a INCREMENTAL materialisation: MERGE instead of load
            merge = self.db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            )
            self.compiled = (
                f"-- Create temporary table\n"
                f"{tmp_table}\n\n"
                f"-- Move data to final table\n"
                f"{merge}\n\n"
            )

        else:
            raise ValueError("SAYN issue")

        return TaskStatus.READY
