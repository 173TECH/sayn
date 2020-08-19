from collections import OrderedDict

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

        status = self._pre_run_checks()
        print(status)
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
            self._exeplan_create(self.table, self.schema, self.sql_query, view=True)
        elif self.materialisation == "table" or (
            self.materialisation == "incremental"
            and (
                self.sayn_config.options["full_load"] is True
                or self.db.table_exists(self.table, self.schema) is False
            )
        ):
            self._exeplan_drop(self.tmp_table, self.tmp_schema)
            self._exeplan_create(
                self.tmp_table, self.tmp_schema, self.sql_query, ddl=self.ddl
            )
            self._exeplan_create_indexes(self.tmp_table, self.tmp_schema, self.ddl)
            self._exeplan_drop(self.table, self.schema)
            self._exeplan_move(
                self.tmp_table, self.tmp_schema, self.table, self.schema, self.ddl
            )
        else:  # incremental not full refresh or incremental table exists
            self._exeplan_drop(self.tmp_table, self.tmp_schema)
            self._exeplan_create(
                self.tmp_table, self.tmp_schema, self.sql_query, ddl=self.ddl
            )
            self._exeplan_merge(
                self.tmp_table,
                self.tmp_schema,
                self.table,
                self.schema,
                self.delete_key,
            )
            self._exeplan_drop(self.tmp_table, self.tmp_schema)

        # permissions
        self._exeplan_set_permissions(self.table, self.schema, self.ddl)

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

    def _pre_run_checks(self):
        # if incremental load and columns not in same order than ddl columns -> stop
        if self.ddl.get("columns") is not None:
            ddl_column_names = [c["name"] for c in self.ddl.get("columns")]
            if (
                self.materialisation == "incremental"
                and self.sayn_config.options["full_load"] is False
                and self.db.table_exists(self.table, self.schema)
            ):
                table_column_names = [
                    c.name for c in self.db.get_table(self.table, self.schema).columns
                ]
                if ddl_column_names != table_column_names:
                    UI().error(
                        "ABORTING: DDL columns in task settings are not in similar order than table columns. Please do a full load of the table."
                    )
                    return TaskStatus.FAILED

        return TaskStatus.READY

    # Task property methods - EXECUTION

    def _exeplan_drop(self, table, schema, view=False):
        UI().debug(
            "Dropping {name}...".format(
                name=schema + "." if schema is not None else "" + table
            )
        )
        stop = False
        try:
            drop1 = self.db.drop_table(table, schema, view=view)
            self.db.execute(drop1)
            stop = True
        except:
            pass

        if stop is False:
            try:
                drop2 = self.db.drop_table(table, schema, view=not view)
                self.db.execute(drop2)
            except:
                pass

    def _exeplan_create(self, table, schema, select, view=False, ddl=dict()):
        UI().debug(
            "Creating {table_or_view} {name}...".format(
                table_or_view="view" if view is True else "table",
                name=schema + "." if schema is not None else "" + table,
            )
        )
        if ddl.get("columns") is None:
            create = self.db.create_table_select(table, schema, select, view=view)
            self.db.execute(create)
        else:
            ddl_column_names = [c["name"] for c in ddl.get("columns")]
            # create table with DDL and insert the output of the select
            create = self.db.create_table_ddl(table, schema, ddl)
            self.db.execute(create)
            # we need to reshape the query to ensure that the columns are always in the right order
            insert = self.db.insert(table, schema, select, columns=ddl_column_names)
            self.db.execute(insert)

    def _exeplan_move(self, src_table, src_schema, dst_table, dst_schema, ddl):
        UI().debug(
            "Moving table {src_name} to {dst_name}...".format(
                src_name=src_schema + "." if src_schema is not None else "" + src_table,
                dst_name=dst_schema + "." if dst_schema is not None else "" + dst_table,
            )
        )
        move = self.db.move_table(src_table, src_schema, dst_table, dst_schema, ddl)
        self.db.execute(move)

    def _exeplan_merge(self, tmp_table, tmp_schema, table, schema, delete_key):
        UI().debug(
            "Merging into table {name}...".format(
                name=schema + "." if schema is not None else "" + table
            )
        )
        merge = self.db.merge_tables(tmp_table, tmp_schema, table, schema, delete_key)
        self.db.execute(merge)

    def _exeplan_create_indexes(self, table, schema, ddl):
        if self.ddl.get("indexes") is not None:
            UI().debug(
                "Creating indexes on table {name}...".format(
                    name=schema + "." if schema is not None else "" + table
                )
            )
            indexes = self.db.create_indexes(table, schema, ddl)
            self.db.execute(indexes)

    def _exeplan_set_permissions(self, table, schema, ddl):
        if ddl.get("permissions") is not None:
            UI().debug(
                "Setting permissions on {name}...".format(
                    name=schema + "." if schema is not None else "" + table
                )
            )
            permissions = self.db.grant_permissions(table, schema, ddl["permissions"])
            self.db.execute(permissions)
