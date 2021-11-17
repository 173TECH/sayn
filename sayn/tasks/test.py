from pathlib import Path

from pydantic import BaseModel, Field, FilePath, validator, Extra
from typing import List, Optional, Union

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Tests(BaseModel):
    name: str

    class Config:
        extra = Extra.forbid


class Columns(BaseModel):
    name: str
    description: Optional[str]
    tests: List[Union[str, Tests]]

    class Config:
        extra = Extra.forbid


class Config(BaseModel):
    columns: List[Columns]

    class Config:
        extra = Extra.forbid


class TestTask(Task):
    def setup(self, **config):
        try:
            self.config = Config(columns=config["columns"])
        except Exception as e:
            return Exc(e)

        cols = self.config.columns
        self.columns = []
        for c in cols:
            print(c)
            self.columns.append(
                {
                    "name": c.name,
                    "description": c.description,
                    "test": [t if isinstance(t, str) else [t.name] for t in c.tests],
                }
            )
        print(self.columns)
        return self.success()

    #
    # def run(self):
    #     self.debug("Nothing to be done")
    #     return self.success()
    #
    # def compile(self):
    #     self.debug("Nothing to be done")
    #     return self.success()

    def test(self):
        # print(2)
        # print(vars(self))

        self.debug("Nothing to be done")
        return self.success()
