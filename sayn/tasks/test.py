from pathlib import Path

from pydantic import BaseModel, FilePath, validator, Extra
from typing import List, Optional, Union
from enum import Enum
from colorama import Fore, Style

from ..core.errors import Ok, Err, Exc
from ..database import Database
from .task import Task


class Tests(BaseModel):
    name: Optional[str]
    values: Optional[List[str]]

    class Config:
        extra = Extra.forbid


class Columns(BaseModel):
    name: str
    description: Optional[str]
    tests: List[Union[str, Tests]]

    class Config:
        extra = Extra.forbid


class Config(BaseModel):
    test_folder: Path
    file_name: FilePath

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["test_folder"], v)


class OnFailValue(str, Enum):
    skip = "skip"
    no_skip = "no_skip"


class CompileConfig(BaseModel):
    tags: Optional[List[str]]
    # sources: Optional[List[str]]
    # outputs: Optional[List[str]]
    # parents: Optional[List[str]]
    # on_fail: Optional[OnFailValue]

    class Config:
        extra = Extra.forbid


class TestTask(Task):
    def config(self, **config):
        self._has_tests = True

        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        print(self.run_arguments["folders"]["tests"])
        # set the target db for execution
        if config.get("db") is not None:
            if config["db"] not in conn_names_list:
                return Err("task_definition", "db_not_in_settings", db=config["db"])
            self._target_db = config["db"]
        else:
            self._target_db = self._default_db

        try:
            self.task_config = Config(
                test_folder=self.run_arguments["folders"]["tests"], **config
            )
        except Exception as e:
            return Exc(e)

        print(self.task_config)
        self.compiler.update_globals(
            src=lambda x: self.src(x, connection=self._target_db),
            config=self.config_macro,
        )

        self.test_query = self.compiler.compile(self.task_config.file_name)
        self.test_query += " LIMIT 5\n"

        return Ok()

    def config_macro(self, **config):
        if self.allow_config:
            task_config_override = CompileConfig(**config)

            # Sent to the wrapper
            # if task_config_override.on_fail is not None:
            #     self._config_input["on_fail"] = task_config_override.on_fail

            if task_config_override.tags is not None:
                self._config_input["tags"] = task_config_override.tags

            # if task_config_override.parents is not None:
            #     self._config_input["parents"] = task_config_override.parents

        # Returns an empty string to avoid productin incorrect sql
        return ""

    def setup(self):
        return Ok()

    def test(self):
        step_queries = {
            "Write Test Query": self.test_query,
            "Execute Test Query": self.test_query,
        }
        if self.run_arguments["command"] == "test":
            self.set_run_steps(list(step_queries.keys()))

        for step, query in step_queries.items():
            with self.step(step):
                if "Write" in step:
                    self.write_compilation_output(query, "test")
                if "Execute" in step:
                    try:
                        result = self.default_db.read_data(query)
                    except Exception as e:
                        return Exc(e)

        if len(result) == 0:
            return self.success()
        else:
            if self.run_arguments["debug"]:
                errout = "Test failed. "
                data = []

                for res in result:
                    for r in list(res.values()):
                        data.append(r)
                data = data[:5]

                errinfo = f"You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"

                return self.fail(errout + errinfo)
            else:
                return self.fail("Failed test types: custom")
