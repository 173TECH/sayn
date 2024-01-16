from datetime import datetime
from typing import Any, Dict, Optional, List, Union
import re

from pydantic import BaseModel, Field, validator, Extra
from sqlalchemy import or_, select, column

from ..core.errors import Err, Exc, Ok
from ..database import Database
from .sql import SqlTask

# from .test import Columns


class Source(BaseModel):
    supports_schemas: bool
    db_type: str

    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    db: str
    db_name: Optional[str]

    class Config:
        extra = Extra.forbid

    @validator("db_schema")
    def can_use_schema(cls, v, values):
        if v is not None and not values["supports_schemas"]:
            raise ValueError(
                f'schema not supported for database of type {values["db_type"]}'
            )

        return v


class Destination(BaseModel):
    supports_schemas: bool
    db_type: str

    tmp_schema: Optional[str]
    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    db: Optional[str]
    db_name: Optional[str]

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


class Config(BaseModel):
    source: Source
    destination: Destination
    # ddl: Optional[Dict[str, Any]]
    delete_key: Optional[str]
    append: bool = False
    incremental_key: Optional[str]
    max_merge_rows: Optional[int]
    max_batch_rows: Optional[int]
    columns: Optional[List[Union[str, Dict[str, Any]]]] = list()
    table_properties: Optional[List[Dict[str, Any]]] = list()
    post_hook: Optional[List[Dict[str, Any]]] = list()

    class Config:
        extra = Extra.forbid

    @validator("incremental_key", always=True)
    def incremental_validation(cls, v, values):
        if v is None:  # Full load
            if values.get("delete_key") is not None:
                raise ValueError(
                    'Incremental copy requires both "incremental_key" and "delete_key" or "incremental_key" and "append: true"'
                )
        else:
            if values.get("delete_key") is not None and values.get("append"):
                raise ValueError(
                    '"Append" incremental copy is incompatible with "delete_key"'
                )
            elif values.get("delete_key") is None and not values.get("append"):
                raise ValueError(
                    '"Append" incremental copy requires "delete_key" or  "append: True"'
                )

        return v

    @validator("max_merge_rows")
    def merge_batch_size_val(cls, v, values):
        if values.get("incremental_key") is None:
            raise ValueError("max_merge_rows is only applicable to incremental copy")

        return v


class CopyTask(SqlTask):
    def config(self, **config):  # noqa: C901
        if "task_name" in self._config_input:
            del self._config_input["task_name"]

        conn_names_list = [
            n for n, c in self.connections.items() if isinstance(c, Database)
        ]

        # check the source db exists in settings
        if (
            isinstance(config.get("source"), dict)
            and config["source"].get("db") is not None
        ):
            if config["source"]["db"] not in conn_names_list:
                return Err(
                    "task_definition",
                    "source_db_not_in_settings",
                    db=config["source"]["db"],
                )

        # set the target db for execution
        # this check needs to happen here so we can pass db_features and db_type to the validator
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

        if isinstance(config.get("source"), dict):
            config["source"].update(
                {
                    "supports_schemas": not self.connections[
                        config["source"]["db"]
                    ].feature("NO SCHEMA SUPPORT"),
                    "db_type": self.connections[config["source"]["db"]].db_type,
                }
            )

        if isinstance(config.get("destination"), dict):
            config["destination"].update(
                {
                    "supports_schemas": not self.target_db.feature("NO SCHEMA SUPPORT"),
                    "db_type": self.target_db.db_type,
                }
            )

        try:
            self.task_config = Config(**config)
        except Exception as e:
            return Exc(e)

        # Setup sources
        self.source_db = self.connections[self.task_config.source.db]
        if (self.task_config.source.db_name is None) and (
            self.task_config.source.db_schema is None
        ):
            self.source_name = None
            self.source_schema = None
            self.source_table = self.src(
                self.task_config.source.table, connection=self.source_db
            )
        elif (self.task_config.source.db_name is None) and (
            self.task_config.source.db_schema is not None
        ):
            self.source_name = None
            obj = self.src(
                f"{self.task_config.source.db_schema}.{self.task_config.source.table}",
                connection=self.source_db,
            )
            self.source_schema = obj.split(".")[0]
            self.source_table = obj.split(".")[1]
        else:
            obj = self.src(
                f"{self.task_config.source.db_name}.{self.task_config.source.db_schema}.{self.task_config.source.table}",
                connection=self.source_db,
            )
            self.source_name = obj.split(".")[0]
            self.source_schema = obj.split(".")[1]
            self.source_table = obj.split(".")[2]

        # Setup outputs
        self.config_tmp_schema = (
            self.task_config.destination.tmp_schema
            or self.task_config.destination.db_schema
        )
        config_db = self.task_config.destination.db_name
        config_schema = self.task_config.destination.db_schema
        config_table = self.task_config.destination.table
        config_tmp_table = f"sayn_tmp_{config_table}"

        if (config_db is None) and (config_schema is None):
            self.database = None
            self.schema = None
            self.table = self.out(config_table, connection=self.target_db)
        elif (config_db is None) and (config_schema is not None):
            obj = self.out(f"{config_schema}.{config_table}", connection=self.target_db)
            self.database = None
            self.schema = obj.split(".")[0]
            self.table = obj.split(".")[1]
        else:
            obj = self.out(
                f"{config_db}.{config_schema}.{config_table}", connection=self.target_db
            )
            self.database = obj.split(".")[0]
            self.schema = obj.split(".")[1]
            self.table = obj.split(".")[2]

        if self.config_tmp_schema is None:
            self.tmp_schema = None
            self.tmp_table = self.out(config_tmp_table, connection=self.target_db)
        else:
            obj = self.out(
                f"{config_schema}.{config_tmp_table}",
                connection=self.target_db,
            )
            self.tmp_schema = obj.split(".")[0]
            self.tmp_table = obj.split(".")[1]

        self.delete_key = self.task_config.delete_key
        self.src_incremental_key = self.task_config.incremental_key
        self.dst_incremental_key = self.task_config.incremental_key
        self.max_merge_rows = self.task_config.max_merge_rows
        self.max_batch_rows = self.task_config.max_batch_rows

        if self.task_config.append:
            self.mode = "append"
        elif self.dst_incremental_key is None:
            self.mode = "full"
        else:
            self.mode = "inc"

        self.is_full_load = self.run_arguments["full_load"] or self.mode == "full"

        result = self.target_db._validate_ddl(
            self.task_config.columns,
            self.task_config.table_properties,
            self.task_config.post_hook,
        )
        if result.is_ok:
            self.columns = result.value

            # Check if the incremental_key in the destination needs renaming
            if (
                self.dst_incremental_key is not None
                and len(self.columns["columns"]) > 0
            ):
                columns_dict = {
                    c["name"]: c["dst_name"] or c["name"]
                    for c in self.columns["columns"]
                }
                self.dst_incremental_key = columns_dict[self.src_incremental_key]
        else:
            return result

        if (
            self.run_arguments["command"] == "test" or self.run_arguments["run_tests"]
        ) and len(self.columns["columns"]) > 0:
            result = self.target_db._construct_tests(
                self.columns["columns"], self.table, self.schema
            )
            if result.is_err:
                return result
            else:
                self.test_query = result.value[0]
                self.test_breakdown = result.value[1]

            if self.test_query is not None:
                self._has_tests = True

        return Ok()

    def setup(self):
        if self.needs_recompile:
            if (self.task_config.source.db_name is None) and (
                self.task_config.source.db_schema is None
            ):
                self.source_name = None
                self.source_schema = None
                self.source_table = self.src(
                    self.task_config.source.table, connection=self.source_db
                )
            elif (self.task_config.source.db_name is None) and (
                self.task_config.source.db_schema is not None
            ):
                self.source_name = None
                obj = self.src(
                    f"{self.task_config.source.db_schema}.{self.task_config.source.table}",
                    connection=self.source_db,
                )
                self.source_schema = obj.split(".")[0]
                self.source_table = obj.split(".")[1]
            else:
                obj = self.src(
                    f"{self.task_config.source.db_name}.{self.task_config.source.db_schema}.{self.task_config.source.table}",
                    connection=self.source_db,
                )
                self.source_name = obj.split(".")[0]
                self.source_schema = obj.split(".")[1]
                self.source_table = obj.split(".")[2]

            if self._has_tests:
                schema = self.task_config.destination.db_schema
                table = self.task_config.destination.table
                obj = self.src(f"{schema}.{table}", self.target_db)
                test_schema = obj.split(".")[0]
                test_table = obj.split(".")[1]
                result = self.target_db._construct_tests(
                    self.ddl["columns"], test_table, test_schema
                )
                if result.is_err:
                    return result
                else:
                    self.test_query = result.value[0]
                    self.test_breakdown = result.value[1]

        return Ok()

    def compile(self):
        result = self.get_columns()
        if result.is_err:
            return result

        return self.execute(False, self.run_arguments["debug"], self.is_full_load)

    def run(self):
        result = self.get_columns()
        if result.is_err:
            return result

        if self.max_merge_rows is not None:
            result = self.execute(
                True,
                self.run_arguments["debug"],
                self.is_full_load,
                self.max_merge_rows,
            )
            if result.is_err:
                return result
            for _ in range(100):
                if result.is_err or result.value < self.max_merge_rows:
                    break
                result = self.execute(True, False, False, self.max_merge_rows)
        else:
            result = self.execute(True, self.run_arguments["debug"], self.is_full_load)

        return result

    def test(self):
        step_queries = {
            "Write Test Query": self.test_query,
            "Execute Test Query": self.test_query,
        }
        breakdown = self.get_test_breakdown(self.test_breakdown)

        if self.test_query is None:
            self.info("Nothing to be done")
            return self.success()
        else:
            self.set_run_steps(list(step_queries.keys()))

            for step, query in step_queries.items():
                with self.step(step):
                    if "Write" in step:
                        self.write_compilation_output(query, "test")
                    if "Execute" in step:
                        try:
                            result = self.target_db.read_data(query)
                        except Exception as e:
                            return Exc(e)

            if len(result) == 0:
                return self.test_sucessful(breakdown)
            else:
                errout, failed = self.test_failure(
                    breakdown, result, self.run_arguments["debug"]
                )
                problematic_values_query = self.target_db.test_problematic_values(
                    failed, self.table, self.schema
                )

                problematic_report = "\n"

                for query in problematic_values_query.split(";"):
                    if query.strip():
                        header = re.search(r"--.*?--", query).group(0)

                        problematic_report += header
                        problematic_report += "\n====================================================================\n"
                        problematic_report += (
                            re.sub(r"--.*?--", "", query).replace("\n", " ").strip()
                            + ";"
                        )
                        problematic_report += "\n====================================================================\n"

                self.write_compilation_output(
                    problematic_values_query, "test_problematic_values"
                )

                errout.error.details["message"] += (
                    problematic_report
                    + f"\n\tTest Failed. You can find the compiled test query at compile/{self.group}/{self.name}_test.sql. You can find queries to retrieve the problematic values at compile/{self.group}/{self.name}_test_problematic_values.sql"
                )

                return errout

    def execute(self, execute, debug, is_full_load, limit=None):
        # Introspect target
        self.target_table_exists = self.target_db._table_exists(self.table, self.schema)

        steps = ["Prepare Load", "Load Data"]
        if self.target_table_exists:
            load_db = self.database
            load_table = self.tmp_table
            load_schema = self.tmp_schema
            if is_full_load or self.mode == "full":
                steps.append("Move Table")
                is_temporary = False
            else:
                steps.append("Merge Tables")
                is_temporary = True
        else:
            load_db = self.database
            load_table = self.table
            load_schema = self.schema
            is_temporary = False

        self.set_run_steps(steps)

        with self.step("Prepare Load"):
            result = self.get_read_query(execute, debug, is_full_load, limit)
            if result.is_err:
                return result
            else:
                get_data_query = result.value

            create_ddl = {k: v for k, v in self.columns.items() if k != "columns"}

            if self.mode == "append":
                create_ddl["columns"] = [c for c in self.columns["columns"]] + [
                    {"name": "_sayn_load_ts", "type": "TIMESTAMP"}
                ]
            else:
                create_ddl["columns"] = [c for c in self.columns["columns"]]

            query = self.target_db.create_table(
                load_table,
                schema=load_schema,
                db=load_db,
                replace=True,
                temporary=is_temporary,
                **create_ddl,
            )
            if debug:
                self.write_compilation_output(query, "create_table")
            if execute:
                try:
                    self.target_db.execute(query)
                except Exception as e:
                    return Exc(e)

        with self.step("Load Data"):
            n_records = 0
            if execute:
                data_iter = self.source_db._read_data_stream(get_data_query)

                def read_iter(iter):
                    if self.mode == "append":
                        load_time = datetime.utcnow()
                        for record in iter:
                            yield dict(record, _sayn_load_ts=load_time)

                    else:
                        for record in iter:
                            yield record

                n_records = self.target_db.load_data(
                    load_table,
                    read_iter(data_iter),
                    db=load_db,
                    schema=load_schema,
                    batch_size=self.max_batch_rows,
                )
        # Final step
        final_step = steps[-1]
        if final_step == "Move Table":
            query = self.target_db.move_table(
                load_table,
                self.table,
                src_schema=load_schema,
                dst_schema=self.schema,
                src_db=self.source_name,
                dst_db=self.database,
                **self.columns,
            )
        elif final_step == "Merge Tables":
            query = self.target_db.merge_tables(
                load_table,
                self.table,
                self.delete_key,
                src_schema=load_schema,
                dst_schema=self.schema,
                src_db=self.source_name,
                dst_db=self.database,
                **self.columns,
            )
        else:
            query = None

        if query is not None:
            with self.step(final_step):
                if debug:
                    self.write_compilation_output(
                        query, final_step.replace(" ", "_").lower()
                    )
                if execute:
                    try:
                        self.target_db.execute(query)
                    except Exception as e:
                        return Exc(e)

        return Ok(n_records)

    def get_columns(self):  # noqa: C901
        # We get the source table definition
        source_table_def = self.source_db._get_table(
            self.source_table,
            self.source_schema,
        )
        if source_table_def is None:
            return Err(
                "database_error",
                "source_db_missing_source_table",
                schema=self.source_schema,
                table=self.source_table,
                db=self.source_db.name,
            )
        self.source_table_def = source_table_def

        if len(self.columns["columns"]) == 0:
            dst_table_def = None
            if not self.is_full_load:
                dst_table_def = self.target_db._get_table(self.table, self.schema)

            if dst_table_def is not None:
                # In incremental loads we use the destination table to determine the columns
                self.columns["columns"] = [
                    {
                        "name": c.name,
                        "type": c.type.compile(dialect=self.target_db.engine.dialect),
                    }
                    for c in dst_table_def.columns
                    if not c.name.startswith("_sayn")
                ]

                # Ensure these columns are in the source
                missing_columns = set(
                    [
                        c.name
                        for c in dst_table_def.columns
                        if not c.name.startswith("_sayn")
                    ]
                ) - set([c.name for c in self.source_table_def.columns])

                if len(missing_columns) > 0:
                    return Err(
                        "database_error",
                        "source_table_missing_columns",
                        db=self.source_db.name,
                        table=self.source_table,
                        schema=self.source_schema,
                        columns=missing_columns,
                    )

            else:
                # In any other case, we use the source
                for c in self.source_table_def.columns:
                    try:
                        col_type = self.target_db._py2sqa(c.type.python_type)
                    except:
                        col_type = c.type.compile(self.target_db.engine.dialect)
                    self.columns["columns"].append({"name": c.name, "type": col_type})
        else:
            # Fill up column types from the source table
            for col in self.columns["columns"]:
                if col.get("name") not in self.source_table_def.columns:
                    return Err(
                        "database_error",
                        "source_table_missing_columns",
                        db=self.source_db.name,
                        table=self.source_table,
                        schema=self.source_schema,
                        column=col.get("name"),
                    )

                if col.get("type") is None:
                    try:
                        col["type"] = self.source_table_def.columns[
                            col["name"]
                        ].type.compile(self.target_db.engine.dialect)
                    except:
                        col["type"] = self.target_db._py2sqa(
                            self.source_table_def.columns[col["name"]].type.python_type
                        )

        for col in self.columns["columns"]:
            col["src_name"] = col["name"]
            if col.get("dst_name") is not None:
                col["name"] = col["dst_name"]

        return Ok()

    def get_read_query(self, execute, debug, is_full_load, limit=None):
        # Get the incremental value
        last_incremental_value_query = (
            f"SELECT MAX({self.dst_incremental_key}) AS value\n"
            f"FROM {'' if self.schema is None else self.schema +'.'}{self.table}\n"
            f"WHERE {self.dst_incremental_key} IS NOT NULL"
        )
        if debug:
            self.write_compilation_output(
                last_incremental_value_query, "last_incremental_value"
            )

        get_data_query = select(
            columns=[
                column(c["src_name"]).label(c["name"])
                if c["src_name"] != c["name"]
                else column(c["src_name"])
                for c in self.columns["columns"]
            ],
            from_obj=self.source_table_def,
        )
        last_incremental_value = None

        if (
            not is_full_load
            and self.target_table_exists
            and self.dst_incremental_key is not None
        ):
            if execute:
                res = self.target_db.read_data(last_incremental_value_query)
                if len(res) == 1:
                    last_incremental_value = res[0]["value"]
            else:
                last_incremental_value = "LAST_INCREMENTAL_VALUE"

        # Select stream
        if last_incremental_value is not None:
            get_data_query = get_data_query.where(
                or_(
                    self.source_table_def.c[self.src_incremental_key].is_(None),
                    self.source_table_def.c[self.src_incremental_key]
                    >= last_incremental_value,
                )
            )

        if self.src_incremental_key is not None:
            get_data_query = get_data_query.order_by(self.src_incremental_key)

        if limit is not None:
            get_data_query = get_data_query.limit(limit)

        if debug:
            try:
                q = get_data_query.compile(compile_kwargs={"literal_binds": True})
            except:
                # compilation can fail when using values like dates
                q = str(get_data_query)
            self.write_compilation_output(q, "get_data")

        return Ok(get_data_query)
