from pathlib import Path

from pydantic import BaseModel, FilePath, validator, Extra
from typing import Optional, List
from enum import Enum

from ..core.errors import Ok, Err, Exc
from ..database import Database
from .task import Task


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath
    db: Optional[str]

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class OnFailValue(str, Enum):
    skip = "skip"
    no_skip = "no_skip"


class CompileConfig(BaseModel):
    tags: Optional[List[str]]
    sources: Optional[List[str]]
    outputs: Optional[List[str]]
    parents: Optional[List[str]]
    on_fail: Optional[OnFailValue]

    class Config:
        extra = Extra.forbid


class SqlTask(Task):
    @property
    def target_db(self):
        return self.connections[self._target_db]

    def config(self, **config):
        if "task_name" in self._config_input:
            del self._config_input["task_name"]

        if "columns" in config:
            self._has_tests = True

        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        # set the target db for execution
        if config.get("db") is not None:
            if config["db"] not in conn_names_list:
                return Err("task_definition", "db_not_in_settings", db=config["db"])
            self._target_db = config["db"]
        else:
            self._target_db = self._default_db

        try:
            self.task_config = Config(
                sql_folder=self.run_arguments["folders"]["sql"], **config
            )
        except Exception as e:
            return Exc(e)

        self.compiler.update_globals(
            src=lambda x: self.src(x, connection=self._target_db),
            out=lambda x: self.out(x, connection=self._target_db),
            config=self.config_macro,
        )

        self.allow_config = True

        self.prepared_sql_query = self.compiler.prepare(self.task_config.file_name)
        self.sql_query = self.prepared_sql_query.compile()
        self.allow_config = False

        if self.run_arguments["command"] == "run":
            self.set_run_steps(["Write Query", "Execute Query"])
        else:
            self.set_run_steps(["Write Query"])

        return Ok()

    def config_macro(self, **config):
        if self.allow_config:
            task_config_override = CompileConfig(**config)
            # Sent to the wrapper
            if task_config_override.on_fail is not None:
                self._config_input["on_fail"] = task_config_override.on_fail

            if task_config_override.tags is not None:
                self._config_input["tags"] = task_config_override.tags

            if task_config_override.sources is not None:
                self._config_input["sources"] = task_config_override.sources

            if task_config_override.outputs is not None:
                self._config_input["outputs"] = task_config_override.outputs

            if task_config_override.parents is not None:
                self._config_input["parents"] = task_config_override.parents

        # Returns an empty string to avoid productin incorrect sql
        return ""

    def setup(self):
        if self.needs_recompile:
            self.sql_query = self.prepared_sql_query.compile()

        return Ok()

    def compile(self):
        with self.step("Write Query"):
            self.write_compilation_output(self.sql_query)

        return Ok()

    def run(self):
        with self.step("Write Query"):
            self.write_compilation_output(self.sql_query)

        with self.step("Execute Query"):
            try:
                self.target_db.execute(self.sql_query)
            except Exception as e:
                return Exc(e)

        return Ok()
