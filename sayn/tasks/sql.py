from pathlib import Path

from pydantic import BaseModel, FilePath, validator
from typing import Optional

from ..core.errors import Ok, Err
from ..database import Database
from .base_sql import BaseSqlTask


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath
    db: Optional[str]

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(BaseSqlTask):
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

        self.config = Config(sql_folder=self.run_arguments["folders"]["sql"], **config)

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        # Set execution steps
        self.steps = ["Write Query"]
        if self.run_arguments["command"] == "run":
            self.steps.append("Execute Query")

        return Ok()
