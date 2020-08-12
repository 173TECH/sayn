from .sql import SqlTask


class AutoSqlTask(SqlTask):
    def setup(self):
        self.db = self.sayn_config.default_db

        status = self._setup_file_name()
        if status != 0:  # TODO TaskStatus.READY:
            return self.failed()

        self.template = self._get_query_template()
        if self.template is None:
            return self.failed()

        status = self._setup_materialisation()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_destination()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_ddl()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        status = self._setup_sql()
        if status != 0:  # TODO TaskStatus.READY:
            return status

        return self._check_extra_fields()

    def _get_compiled_query(self):
        if self.materialisation == "view":
            compiled = self.create_query
        # table | incremental when table does not exit or full load
        elif self.materialisation == "table" or (
            self.materialisation == "incremental"
            and (
                not self.db.table_exists(self.table, self.schema)
                or self.sayn_config.options["full_load"]
            )
        ):
            compiled = f"{self.create_query}\n\n-- Move table\n{self.move_query}"
        # incremental in remaining cases
        else:
            compiled = f"{self.create_query}\n\n-- Merge table\n{self.merge_query}"

        if self.permissions_query is not None:
            compiled += f"\n-- Grant permissions\n{self.permissions_query}"

        return compiled

    def run(self):
        self.compiled = self._get_compiled_query()
        return super(AutoSqlTask, self).run()

    def compile(self):
        self.compiled = self._get_compiled_query()
        return super(AutoSqlTask, self).compile()

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

        return  # TODO return TaskStatus.READY

    def _setup_sql(self):
        # Autosql tasks are split in 4 queries depending on the materialisation
        # 1. Create: query to create and load data
        self.create_query = None
        # 2. Move: query to moves the tmp table to the target one
        self.move_query = None
        # 3. Merge: query to merge data into the target object
        self.merge_query = None
        # 4. Permissions: grants permissions if necessary
        self.permissions_query = None

        # 0. Retrieve the select statement compiled with jinja
        try:
            query = self.template.render(**self.parameters)
        except Exception as e:
            return self.failed(f"Error compiling template\n{e}")

        # 1. Create query
        if self.materialisation == "view":
            # Views just replace the current object if it exists
            self.create_query = self.db.create_table_select(
                self.table, self.schema, query, replace=True, view=True
            )
        else:
            # We always load data into a temporary table
            if self.ddl.get("columns") is not None:
                # create table with DDL and insert the output of the select
                create_table_ddl = self.db.create_table_ddl(
                    self.tmp_table, self.tmp_schema, self.ddl, replace=True
                )
                insert = self.db.insert(self.table, self.schema, query)
                create_load = (
                    f"-- Create table\n"
                    f"{create_table_ddl}\n\n"
                    f"-- Load data\n"
                    f"{insert}\n\n"
                )

            else:
                # or create from the select if DDL not present
                create_load = self.db.create_table_select(
                    self.tmp_table, self.tmp_schema, query, replace=True, ddl=self.ddl
                )

            # Add indexes if necessary
            if self.ddl.get("indexes") is not None:
                indexes = "\n-- Create indexes\n" + self.db.create_indexes(
                    self.tmp_table, self.tmp_schema, self.ddl
                )
            else:
                indexes = ""

            self.create_query = create_load + "\n" + indexes

        # 2. Move
        self.move_query = self.db.move_table(
            self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl
        )

        # 3. Merge
        if self.materialisation == "incremental":
            self.merge_query = self.db.merge_tables(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            )

        # Permissions are always the same
        if self.ddl.get("permissions") is not None:
            self.permissions_query = self.db.grant_permissions(
                self.table, self.schema, self.ddl["permissions"]
            )

        return  # TODO return TaskStatus.READY
