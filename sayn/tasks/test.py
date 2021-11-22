from pathlib import Path
import json

from pydantic import BaseModel, Field, FilePath, validator, Extra
from typing import List, Optional, Union

from ..core.errors import Ok, Err, Exc
from ..database import Database
from . import Task


class Destination(BaseModel):
    supports_schemas: bool
    db_type: str
    db: Optional[str]
    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str

    class Config:
        extra = Extra.forbid

    @validator("tmp_schema")
    def can_use_tmp_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'tmp_schema not supported for database of type {values["db_type"]}'
            )

        return v

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class Tests(BaseModel):
    name: str
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
    columns: List[Columns]
    destination: Destination

    class Config:
        extra = Extra.forbid


class TestTask(Task):
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
        if self.type == "autosql":
            conn_names_list = [
                n for n, c in self.connections.items() if isinstance(c, Database)
            ]

            if (
                isinstance(config.get("destination"), dict)
                and config["destination"].get("db") is not None
            ):
                if config["destination"]["db"] not in conn_names_list:
                    return Err(
                        "task_definition",
                        "destination_db_not_in_settings",
                        db=config["destination"]["db"],
                    )
                self._target_db = config["destination"]["db"]
            else:
                self._target_db = self._default_db

            if isinstance(config.get("destination"), dict):
                config["destination"].update(
                    {
                        "supports_schemas": not self.target_db.feature(
                            "NO SCHEMA SUPPORT"
                        ),
                        "db_type": self.target_db.db_type,
                    }
                )

        try:
            self.config = Config(
                columns=config["columns"], destination=config["destination"]
            )
        except Exception as e:
            return Exc(e)

        self.tmp_schema = (
            self.config.destination.tmp_schema or self.config.destination.db_schema
        )
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.use_db_object(self.table, schema=self.schema, tmp_schema=self.tmp_schema)

        cols = self.config.columns

        self.columns = []
        for c in cols:
            tests = []
            for t in c.tests:
                if isinstance(t, str):
                    tests.append({"type": t, "values": None})
                else:
                    tests.append({"type": t.name, "values": t.values})
            self.columns.append(
                {
                    "name": c.name,
                    "description": c.description,
                    "tests": tests,
                }
            )

        columns = self.columns
        # print(columns)
        table = self.table
        query = """
                   SELECT col
                        , cnt AS 'count'
                        , type
                     FROM (
                """
        for col in columns:
            tests = col["tests"]
            for t in tests:
                if "unique" in t.values():
                    query += f"""
                            SELECT CAST(l.{ col['name'] } AS VARCHAR) AS col
                                 , COUNT(*) AS cnt
                                 , 'unique' AS type
                              FROM { table } AS l
                             GROUP BY l.{ col['name'] }
                            HAVING COUNT(*) > 1

                            UNION ALL
                            """
                if "not_null" in t.values():
                    query += f"""
                            SELECT CAST(l.{ col['name'] } AS VARCHAR) AS col
                                 , COUNT(*) AS cnt
                                 , 'null' AS type
                              FROM { table } AS l
                             WHERE l.{ col['name'] } IS NULL
                             GROUP BY l.{ col['name'] }
                            HAVING COUNT(*) > 0

                            UNION ALL
                            """
                if "values" in t.values():
                    query += f"""
                            SELECT CAST(l.{ col['name'] } AS VARCHAR) AS col
                                 , COUNT(*) AS cnt
                                 , 'valid_values' AS type
                              FROM { table } AS l
                             WHERE l.{ col['name'] } NOT IN ( {', '.join(f"'{c}'" for c in t['values'])} )
                             GROUP BY l.{ col['name'] }
                            HAVING COUNT(*) > 0

                            UNION ALL
                            """
        parts = query.splitlines()[:-2]
        query = ""
        for q in parts:
            query += q.strip() + "\n"

        query += ") AS t;"

        self.query = query

        # print(query)

        return self.success()

    def test(self):
        with self.step("Write Test Query"):
            result = self.write_compilation_output(self.query, "test")
            if result.is_err:
                return result

        with self.step("Execute Test Query"):
            result = self.default_db.read_data(self.query)

            if len(result) == 0:
                return self.success()
            else:
                errout = "Test failed, problematic fields:\n"
                for res in result:
                    errout += json.dumps(res)
                return self.fail(errout)
