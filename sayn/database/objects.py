from __future__ import annotations

import re
from collections import deque
from enum import Enum
from typing import Mapping, Optional, Set

from . import Database


class ReferenceLevel(Enum):
    database = -2, "db"
    schema = -1, "schema"
    table = 0, None

    def __new__(cls, *values):
        obj = object.__new__(cls)
        # first value is canonical value
        obj._value_ = values[0]
        for other_value in values[1:]:
            cls._value2member_map_[other_value] = obj
        obj._all_values = values
        return obj

    def __repr__(self):
        return "<%s.%s: %s>" % (
            self.__class__.__name__,
            self._name_,
            ", ".join([repr(v) for v in self._all_values]),
        )


class DbObject:
    def __init__(
        self,
        compiler: DbObjectCompiler,
        connection_name: str,
        database: Optional[str],
        schema: Optional[str],
        table: str,
    ) -> None:
        self.compiler = compiler
        self.connection_name = connection_name
        self.database = database
        self.schema = schema
        self.table = table

        # The raw value is the value we use in our project code
        self.raw = ""
        if self.database is not None:
            self.raw += self.database + "."

        if self.schema is not None:
            self.raw += self.schema + "."

        if self.table is not None:
            self.raw += self.table

        # The key is prefixed with the connection name to make objects
        # unique across all databases
        self.key = f"{connection_name}:{self.raw}"

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, obj: DbObject) -> bool:
        return self.key == obj.key

    def __lt__(self, obj: DbObject) -> bool:
        return self.key > obj.key

    def __repr__(self) -> str:
        return f"DbObject: {self.key}"


class DbObjectCompiler:

    regex_obj = re.compile(
        r"((?P<connection>[^:]+):)?((?P<c1>[^.]+)\.)?((?P<c2>[^.]+)\.)?(?P<c3>[^.]+)(?P<dots>\.{0,2})"
    )

    def __init__(
        self,
        connections: Mapping[str, Database],
        default_db: str,
        stringify: Mapping[str, Optional[str]],
        prod_stringify: Mapping[str, Optional[str]],
        from_prod: Set[str],
    ) -> None:
        self.connections = connections
        self.default_db = default_db
        self.from_prod = set()
        self.sources_from_prod = set()

        stringify = {k: v or prod_stringify[k] for k, v in stringify.items()}
        self.stringify = {
            t: self._get_stringify(
                t,
                stringify[f"{t}_prefix"],
                stringify[f"{t}_suffix"],
                stringify[f"{t}_override"],
            )
            for t in ("database", "schema", "table")
        }
        self.prod_stringify = {
            t: self._get_stringify(
                t,
                prod_stringify[f"{t}_prefix"],
                prod_stringify[f"{t}_suffix"],
                prod_stringify[f"{t}_override"],
            )
            for t in ("database", "schema", "table")
        }

        self.from_prod = {re.compile(o) for o in from_prod}

    def _get_stringify(
        self,
        type_: str,
        prefix: Optional[str],
        suffix: Optional[str],
        override: Optional[str],
    ) -> str:
        """Builds the final stringify"""

        # The default is a jinja template with the object name, meaning no modifications to the input
        stringify = "{" + type_ + "}"

        if override is not None:
            # if *_override is defined, use that
            stringify = override
        else:
            if prefix is not None and len(prefix) > 0:
                stringify = prefix + "_" + stringify

            if suffix is not None and len(suffix) > 0:
                stringify = stringify + "_" + suffix
        return stringify

    def set_sources_from_prod(self, sources_from_prod: Set[DbObject]) -> None:
        self.sources_from_prod = sources_from_prod

    def is_from_prod(self, obj: DbObject) -> bool:
        if obj.connection_name == self.default_db:
            # Only for src, we might want to produce the prod version of the object
            for regex in self.from_prod:
                # Only usage of the raw value of a db object.
                # We use raw as all specifications in the project code are meant
                # to follow sayn logic, rather than the db specific special nomenclature
                m = regex.match(obj.raw)
                if m is not None:
                    return True

            for source in self.sources_from_prod:
                if source == obj:
                    return True

        return False

    def _common_value(self, obj: DbObject, run_sensitive: bool) -> str:
        database = obj.database
        schema = obj.schema
        table = obj.table

        if obj.connection_name == self.default_db:
            if run_sensitive:
                is_prod = self.is_from_prod(obj)
            else:
                is_prod = False

            if database is not None:
                if is_prod:
                    database = self.prod_stringify["database"].format(database=database)
                else:
                    database = self.stringify["database"].format(database=database)

            if schema is not None:
                if is_prod:
                    schema = self.prod_stringify["schema"].format(schema=schema)
                else:
                    schema = self.stringify["schema"].format(schema=schema)

            if table is not None:
                if is_prod:
                    table = self.prod_stringify["table"].format(table=table)
                else:
                    table = self.stringify["table"].format(table=table)

        return self.connections[obj.connection_name]._obj_str(database, schema, table)

    def src_obj(self, obj: DbObject) -> DbObject:
        """Returns the transform object according to from_prod rules"""
        return self.from_string(
            self._common_value(obj, True), connection=obj.connection_name
        )

    def out_obj(self, obj: DbObject) -> DbObject:
        """Returns the transform object according to from_prod rules"""
        return self.from_string(
            self._common_value(obj, False), connection=obj.connection_name
        )

    def src_value(self, obj: DbObject) -> str:
        return self._common_value(obj, True)

    def out_value(self, obj: DbObject) -> str:
        return self._common_value(obj, False)

    def from_string(
        self, obj: str, connection: str | Database = None, level: str = None
    ) -> DbObject:
        """This is the entry point to db objects from a SAYN project. Accepting
        2 or 3 components is specified here.
        """
        match = self.regex_obj.match(obj)

        if match is None:
            raise ValueError(f'Incorrect format for database object "{obj}"')

        if isinstance(connection, Database):
            in_connection_name = connection.name
        else:
            in_connection_name = connection

        groups = match.groupdict()
        if groups["connection"] is None:
            if connection is None:
                connection_name = self.default_db
            else:
                connection_name = in_connection_name
        else:
            if connection is None:
                connection_name = groups["connection"]
            else:
                if groups["connection"] != in_connection_name:
                    raise ValueError("Connection name mismatch")
                else:
                    connection_name = in_connection_name

        if groups["c1"] and not groups["c2"]:
            groups["c2"] = groups["c1"]
            groups["c1"] = None

        provided_level = level or -1 * (groups["dots"].count("."))
        reference_level = ReferenceLevel(provided_level)

        component_elements = deque(
            [v for k, v in groups.items() if k != "connection" and k != "dots"]
        )

        component_elements.rotate(reference_level.value)

        components = dict(
            {"table": None, "schema": None, "database": None},
            **dict(
                zip(
                    ("table", "schema", "database"),
                    reversed(component_elements),
                )
            ),
        )

        if components["table"] is None and reference_level.value == 0:
            # This can't happen given the regexp, but this case avoids
            # static analysis errors
            raise ValueError("Error interpreting object string")

        return DbObject(
            self,
            connection_name,
            components["database"],
            components["schema"],
            components["table"],
        )
