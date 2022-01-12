from pathlib import Path
import json

from pydantic import BaseModel, Field, FilePath, validator, Extra
from typing import List, Optional, Union
from terminaltables import AsciiTable

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Config(BaseModel):
    test_folder: Path
    file_name: FilePath

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["test_folder"], v)


class TestTask(Task):
    def setup(self, **config):
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
            self.config = Config(
                test_folder=self.run_arguments["folders"]["tests"], **config
            )
        except Exception as e:
            return Exc(e)

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.test_query = result.value
            self.test_query += " LIMIT 5\n"

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
                            errout = "Test failed, summary:\n"
                            data = []
                            # data.append(["Failed Fields", "Count", "Test Type"])
                            for res in result:
                                data.append(list(res.values()))
                            table = AsciiTable(data)

                            errinfo = f"You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"

                            return self.fail(errout + table.table + "\n\n" + errinfo)
                        else:
                            return self.fail(f"Failed test types: custom")

        # with self.step("Write Test Query"):
        #     result = self.write_compilation_output(self.test_query, "test")
        #     if result.is_err:
        #         return result

        # with self.step("Execute Test Query"):
        #     result = self.default_db.read_data(self.test_query)
        #
        #     if len(result) == 0:
        #         return self.success()
        #     else:
        #         errout = "Test failed, summary:\n"
        #         data = []
        #         # data.append(["Failed Fields", "Count", "Test Type"])
        #         for res in result:
        #             data.append(list(res.values()))
        #         table = AsciiTable(data)
        #
        #         errinfo = f"You can find the compiled test query at compile/{self.group}/{self.name}_test.sql"
        #
        #         return self.fail(errout + table.table + "\n\n" + errinfo)
