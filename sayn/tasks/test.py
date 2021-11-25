from pathlib import Path
import json

from pydantic import BaseModel, Field, FilePath, validator, Extra
from typing import List, Optional, Union

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


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

        return self.success()

    def test(self):
        with self.step("Write Test Query"):
            result = self.write_compilation_output(self.test_query, "test")
            if result.is_err:
                return result

        with self.step("Execute Test Query"):
            result = self.default_db.read_data(self.test_query)

            if len(result) == 0:
                return self.success()
            else:
                errout = "Test failed, problematic fields:\n"
                for res in result:
                    errout += json.dumps(res)
                return self.fail(errout)
