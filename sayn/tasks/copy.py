from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, validator
from sqlalchemy import or_, select, column

from ..core.errors import Err, Exc, Ok
from ..database import Database
from .sql import SqlTask


class Source(BaseModel):
    supports_schemas: bool
    db_type: str

    db_schema: Optional[str] = Field(None, alias="schema")
    table: str
    db: str

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
    ddl: Optional[Dict[str, Any]]
    delete_key: Optional[str]
    incremental_key: Optional[str]
    merge_batch_size: Optional[int]

    @validator("incremental_key", always=True)
    def incremental_validation(cls, v, values):
        if (v is None) != (values.get("delete_key") is None):
            raise ValueError(
                'Incremental copy requires both "delete_key" and "incremental_key"'
            )

        return v

    @validator("merge_batch_size")
    def merge_batch_size_val(cls, v, values):
        if values.get("delete_key") is None:
            raise ValueError("merge_batch_size is only applicable to incremental copy")

        return v


class CopyTask(SqlTask):
    def setup(self, **config):
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
                    "cannot_change_schema": self.connections[
                        config["source"]["db"]
                    ].feature("CANNOT CHANGE SCHEMA"),
                    "db_type": self.connections[config["source"]["db"]].db_type,
                }
            )

        if isinstance(config.get("destination"), dict):
            config["destination"].update(
                {
                    "supports_schemas": not self.target_db.feature("NO SCHEMA SUPPORT"),
                    "cannot_change_schema": self.target_db.feature(
                        "CANNOT CHANGE SCHEMA"
                    ),
                    "db_type": self.target_db.db_type,
                }
            )

        try:
            self.config = Config(**config)
        except Exception as e:
            return Exc(e)

        self.source_db = self.connections[self.config.source.db]
        self.source_schema = self.config.source.db_schema
        self.source_table = self.config.source.table
        self.use_db_object(
            self.source_table,
            schema=self.source_schema,
            db=self.source_db,
            request_tmp=False,
        )

        self.tmp_schema = (
            self.config.destination.tmp_schema or self.config.destination.db_schema
        )
        self.schema = self.config.destination.db_schema
        self.table = self.config.destination.table
        self.tmp_table = f"sayn_tmp_{self.table}"

        self.use_db_object(
            self.table,
            schema=self.schema,
            tmp_schema=self.tmp_schema,
            db=self.target_db,
            request_tmp=True,
        )

        self.delete_key = self.config.delete_key
        self.incremental_key = self.config.incremental_key
        self.merge_batch_size = self.config.merge_batch_size
        self.is_full_load = self.run_arguments["full_load"] or self.delete_key is None

        result = self.target_db._validate_ddl(self.config.ddl)
        if result.is_ok:
            self.ddl = result.value
        else:
            return result

        return Ok()

    def compile(self):
        return self.execute(False, self.run_arguments["debug"], self.is_full_load)

    def run(self):
        if self.merge_batch_size is not None:
            result = self.execute(
                True,
                self.run_arguments["debug"],
                self.is_full_load,
                self.merge_batch_size,
            )
            if result.is_err:
                return result
            while True:
                if result.is_err or result.value < self.merge_batch_size:
                    break
                result = self.execute(True, False, False, self.merge_batch_size)
        else:
            result = self.execute(True, self.run_arguments["debug"], self.is_full_load)

        return result

    def execute(self, execute, debug, is_full_load, limit=None):
        # Introspect target
        self.target_table_exists = self.target_db._table_exists(self.table, self.schema)

        steps = ["Prepare Load", "Load Data"]
        if self.target_table_exists:
            load_table = self.tmp_table
            load_schema = self.tmp_schema
            if is_full_load or self.incremental_key is None:
                steps.append("Move Table")
            else:
                steps.append("Merge Tables")
        else:
            load_table = self.table
            load_schema = self.schema

        self.set_run_steps(steps)

        with self.step("Prepare Load"):
            result = self.get_columns()
            if result.is_err:
                return result

            result = self.get_read_query(execute, debug, is_full_load, limit)
            if result.is_err:
                return result
            else:
                get_data_query = result.value

            query = self.target_db.create_table(
                load_table, schema=load_schema, replace=True, **self.ddl
            )
            if debug:
                self.write_compilation_output(query, "create_table")
            if execute:
                try:
                    self.target_db.execute(query)
                except Exception as e:
                    return Exc(e)

        with self.step("Load Data"):
            if execute:
                data_iter = self.source_db._read_data_stream(get_data_query)
                n_records = self.target_db.load_data(
                    load_table, data_iter, schema=load_schema
                )

        # Final step
        final_step = steps[-1]
        if final_step == "Move Table":
            query = self.target_db.move_table(
                load_table,
                self.table,
                src_schema=load_schema,
                dst_schema=self.schema,
                **self.ddl,
            )
        elif final_step == "Merge Tables":
            query = self.target_db.merge_tables(
                load_table,
                self.table,
                self.delete_key,
                src_schema=load_schema,
                dst_schema=self.schema,
                **self.ddl,
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

    def get_columns(self):
        # We get the source table definition
        source_table_def = self.source_db._get_table(
            self.source_table,
            self.source_schema,
            # columns=[c["name"] for c in self.ddl["columns"]],
            # required_existing=True,
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

        if len(self.ddl["columns"]) == 0:
            dst_table_def = None
            if not self.is_full_load:
                dst_table_def = self.target_db._get_table(self.table, self.schema)

            if dst_table_def is not None:
                # In incremental loads we use the destination table to determine the columns
                self.ddl["columns"] = [
                    {
                        "name": c.name,
                        "type": c.type.compile(dialect=self.target_db.engine.dialect),
                    }
                    for c in dst_table_def.columns
                ]

                # Ensure these columns are in the source
                missing_columns = set([c.name for c in dst_table_def.columns]) - set(
                    [c.name for c in self.source_table_def.columns]
                )
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
                self.ddl["columns"] = [
                    {"name": c.name, "type": self.target_db._py2sqa(c.type.python_type)}
                    for c in self.source_table_def.columns
                ]
        else:
            # Fill up column types from the source table
            for col in self.ddl["columns"]:
                if col.get("name") not in self.source_table_def.columns:
                    return Err(
                        "database_error",
                        "source_table_missing_column",
                        db=self.source_db.name,
                        table=self.source_table,
                        schema=self.source_schema,
                        column=col.get("name"),
                    )

                if col.get("type") is None:
                    col["type"] = self.target_db._py2sqa(
                        self.source_table_def.columns[col["name"]].type.python_type
                    )

        return Ok()

    def get_read_query(self, execute, debug, is_full_load, limit=None):
        # Get the incremental value
        last_incremental_value_query = (
            f"SELECT MAX({self.incremental_key}) AS value\n"
            f"FROM {'' if self.schema is None else self.schema +'.'}{self.table}\n"
            f"WHERE {self.incremental_key} IS NOT NULL"
        )
        if debug:
            self.write_compilation_output(
                last_incremental_value_query, "last_incremental_value"
            )

        get_data_query = select(
            columns=[column(c["name"]) for c in self.ddl["columns"]],
            from_obj=self.source_table_def,
        )
        last_incremental_value = None

        if not is_full_load and self.target_table_exists:
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
                    self.source_table_def.c[self.incremental_key].is_(None),
                    self.source_table_def.c[self.incremental_key]
                    > last_incremental_value,
                )
            )
        if debug:
            self.write_compilation_output(get_data_query, "get_data")

        if self.incremental_key is not None:
            get_data_query = get_data_query.order_by(self.incremental_key)

        if limit is not None:
            get_data_query = get_data_query.limit(limit)

        return Ok(get_data_query)
