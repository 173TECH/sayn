from pathlib import Path
import json

from pydantic import BaseModel, FilePath, validator, Extra
from typing import Optional, List
from terminaltables import AsciiTable

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Config(BaseModel):
    sql_folder: Path
    file_name: FilePath
    db: Optional[str]

    class Config:
        extra = Extra.forbid

    @validator("file_name", pre=True)
    def file_name_plus_folder(cls, v, values):
        return Path(values["sql_folder"], v)


class SqlTask(Task):
    @property
    def target_db(self):
        return self.connections[self._target_db]

    def use_db_object(
        self, name, schema=None, tmp_schema=None, db=None, request_tmp=True
    ):
        if db is None:
            target_db = self.target_db
        elif isinstance(db, str):
            target_db = self.connections[db]
        elif isinstance(db, Database):
            target_db = db
        else:
            return Err("use_db_object", "wrong_connection_type")

        target_db._request_object(
            name,
            schema=schema,
            tmp_schema=tmp_schema,
            task_name=self.name,
            request_tmp=request_tmp,
        )

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
                sql_folder=self.run_arguments["folders"]["sql"], **config
            )
        except Exception as e:
            return Exc(e)

        result = self.compile_obj(self.config.file_name)
        if result.is_err:
            return result
        else:
            self.sql_query = result.value

        if self.run_arguments["command"] == "run":
            self.set_run_steps(["Write Query", "Execute Query"])
        else:
            self.set_run_steps(["Write Query"])

        # if isinstance(config.get("columns"), list):
        #     cols = self.config.columns
        #
        #     self.columns = []
        #     for c in cols:
        #         tests = []
        #         for t in c.tests:
        #             if isinstance(t, str):
        #                 tests.append({"type": t, "values": []})
        #             else:
        #                 tests.append(
        #                     {
        #                         "type": t.name if t.name is not None else "values",
        #                         "values": t.values if t.values is not None else [],
        #                     }
        #                 )
        #         self.columns.append(
        #             {
        #                 "name": c.name,
        #                 "description": c.description,
        #                 "tests": tests,
        #             }
        #         )
        #
        #     columns = self.columns
        #     table = self.table
        #     query = """
        #                SELECT col
        #                     , cnt AS 'count'
        #                     , type
        #                  FROM (
        #             """
        #     template = self.get_template(
        #         Path(__file__).parent / "tests/standard_tests.sql"
        #     )
        #     for col in columns:
        #         tests = col["tests"]
        #         for t in tests:
        #             query += self.compile_obj(
        #                 template.value,
        #                 **{
        #                     "table": table,
        #                     "name": col["name"],
        #                     "type": t["type"],
        #                     "values": ", ".join(f"'{c}'" for c in t["values"]),
        #                 },
        #             ).value
        #     parts = query.splitlines()[:-2]
        #     query = ""
        #     for q in parts:
        #         query += q.strip() + "\n"
        #     query += ") AS t;"
        #
        #     self.test_query = query
        #
        return Ok()

    def compile(self):
        with self.step("Write Query"):
            result = self.write_compilation_output(self.sql_query)
            if result.is_err:
                return result

        return Ok()

    def run(self):
        with self.step("Write Query"):
            result = self.write_compilation_output(self.sql_query)
            if result.is_err:
                return result

        with self.step("Execute Query"):
            try:
                self.target_db.execute(self.sql_query)
            except Exception as e:
                return Exc(e)

        return Ok()

    # def test(self):
    #     with self.step("Write Test Query"):
    #         result = self.write_compilation_output(self.test_query, "test")
    #         if result.is_err:
    #             return result
    #
    #     with self.step("Execute Test Query"):
    #         result = self.default_db.read_data(self.test_query)
    #
    #         if len(result) == 0:
    #             return self.success()
    #         else:
    #             errout = "Test failed, summary:\n"
    #             data = []
    #             data.append(['Failed Fields', 'Count', 'Test Type'])
    #             for res in result:
    #                 data.append(list(res.values()))
    #             table = AsciiTable(data)
    #
    #             return self.fail(errout + table.table)
