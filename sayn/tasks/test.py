from pathlib import Path

from pydantic import BaseModel, FilePath, validator, Extra
from typing import List, Optional, Union
from terminaltables import AsciiTable

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


class TestTask(Task):
    def config(self, **config):
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
                test_folder=self.run_arguments["folders"]["tests"], **config
            )
        except Exception as e:
            return Exc(e)

        self.test_query = self.compiler.compile(self.task_config.file_name)
        self.test_query += " LIMIT 5\n"

        if self.run_arguments["command"] == "test":
            self.set_run_steps(["Write Query", "Execute Query"])

        return Ok()

    def setup(self, needs_recompile):
        return Ok()

    def test(self):
        with self.step("Write Test Query"):
            self.write_compilation_output(self.test_query, "test")

        with self.step("Execute Test Query"):
            result = self.default_db.read_data(self.test_query)

            if len(result) == 0:
                return self.success()
            else:
                errout = "Test failed, summary:\n"
                data = []
                # data.append(["Failed Fields", "Count", "Test Type"])
                for res in result:
                    data.append(list(res.values()))
                table = AsciiTable(data)

                errinfo = f"You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"

                return self.fail(errout + table.table + "\n\n" + errinfo)

        return Ok()
