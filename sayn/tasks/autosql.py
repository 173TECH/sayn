from pathlib import Path
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, FilePath, validator

from .sql import SqlTask
from . import TaskStatus


class Destination(BaseModel):
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    _db_properties: List
    _db_type: str

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None:
            raise ValueError(
                f'tmp_schema not supported for database of type {v["_db_type"]}'
            )

        return v


class Config(BaseModel):

    sql_folder: Path
    file_name: FilePath
    delete_key: Optional[str]
    materialisation: str
    destination: Destination
    ddl: Optional[Dict[str, Any]]

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)

    @validator("materialisation")
    def incremental_has_delete_key(cls, v, values):
        if v not in ("table", "view", "incremental"):
            raise ValueError(
                'Accepted materialisations: "table", "view" and "incremental".'
            )
        elif v != "incremental" and values.get("delete_key") is not None:
            raise ValueError("delete_key is not valid for non-incremental loads.")
        elif v == "incremental" and values.get("delete_key") is None:
            raise ValueError("delete_key is required for incremental materialisation.")
        else:
            return v


class AutoSqlTask(SqlTask):
    def setup(self, **kwargs):
        # TODO control this better
        kwargs["destination"].update(
            {
                "_db_features": self.default_db.sql_features,
                "_db_type": self.default_db.db_type,
            }
        )
        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"], **kwargs)
        self.materialisation = self.config.materialisation
        self.tmp_schema = self.config.destination.tmp_schema
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"
        self.delete_key = self.config.delete_key
        self.ddl = self.default_db.validate_ddl(self.config.ddl)
        self.template = self.get_template(self.config.file_name)

        return self.ready()

    def run(self):
        self.set_run_steps(["write_query_on_disk", "execute_sql"])

        # Compilation
        self.logger.debug("Writting query on disk...")
        self.compile()

        # Execution
        self.logger.debug("Executing AutoSQL task...")

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
            )
            self._exeplan_drop(self.tmp_table, self.tmp_schema)

        # permissions
        self._exeplan_set_permissions(self.table, self.schema, ddl=self.ddl)

        return self.success()

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
                    self.logger.error(
                        "ABORTING: DDL columns in task settings are not in similar order than table columns. Please do a full load of the table."
                    )
                    return TaskStatus.FAILED

        return TaskStatus.READY
