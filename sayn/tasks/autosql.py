from .sql import SqlTask
from .task import TaskStatus
from ..utils.ui import UI


class AutoSqlTask(SqlTask):
    # Core

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

        status = self._setup_query()
        if status != TaskStatus.READY:
            return status

        return self._check_extra_fields()

    def run(self):
        # Compilation
        UI().debug("Writting query on disk...")
        self.compile()

        # Execution
        UI().debug("Executing AutoSQL task...")

        if self.materialisation == "view":
            self._exeplan_drop(self.table, self.schema, view=True)
            self._exeplan_create(
                self.table, self.schema, select=self.sql_query, view=True
            )
        elif self.materialisation == "table" or (
            self.materialisation == "incremental"
            and (
                self.sayn_config.options["full_load"] is True
                or self.db.table_exists(self.table, self.schema) is False
            )
        ):
            self._exeplan_drop(self.tmp_table, self.tmp_schema)
            self._exeplan_create(
                self.tmp_table, self.tmp_schema, select=self.sql_query, ddl=self.ddl
            )
            self._exeplan_create_indexes(self.tmp_table, self.tmp_schema, ddl=self.ddl)
            self._exeplan_drop(self.table, self.schema)
            self._exeplan_move(
                self.tmp_table, self.tmp_schema, self.table, self.schema, ddl=self.ddl
            )
        else:  # incremental not full refresh or incremental table exists
            self._exeplan_drop(self.tmp_table, self.tmp_schema)
            self._exeplan_create(
                self.tmp_table, self.tmp_schema, select=self.sql_query, ddl=self.ddl
            )
            self._exeplan_merge(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
                ddl=self.ddl,
            )
            self._exeplan_drop(self.tmp_table, self.tmp_schema)

        # permissions
        self._exeplan_set_permissions(self.table, self.schema, ddl=self.ddl)

        return self.success()

    # Task property methods - SETUP

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
