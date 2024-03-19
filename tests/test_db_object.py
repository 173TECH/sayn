import pytest

from sayn.database.objects import DbObject, DbObjectCompiler
from sayn.database import Database


connetions = {"warehouse": Database("test", "test", "dummy", dict(), dict())}


def get_stringify(input: dict() = dict()):
    input_stringify = {
        "database_prefix": None,
        "database_suffix": None,
        "database_override": None,
        "schema_prefix": None,
        "schema_suffix": None,
        "schema_override": None,
        "table_prefix": None,
        "table_suffix": None,
        "table_override": None,
    }
    input_stringify.update(input)
    return input_stringify


default_db = "warehouse"

from_prod = list()


def test_object_init():
    compiler = DbObjectCompiler(
        connetions, default_db, get_stringify(), get_stringify(), from_prod
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    assert repr(object) == "DbObject: warehouse:db.schema.table"


def test_object_database_input_stringify():
    input_stringify = {"database_prefix": "pre", "database_suffix": "suf"}
    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(input_stringify),
        get_stringify(),
        from_prod,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "pre_db_suf.schema.table"


def test_object_schema_input_stringify():
    input_stringify = {"schema_prefix": "pre", "schema_suffix": "suf"}
    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(input_stringify),
        get_stringify(),
        from_prod,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "db.pre_schema_suf.table"


def test_object_table_input_stringify():
    input_stringify = {"table_prefix": "pre", "table_suffix": "suf"}

    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(input_stringify),
        get_stringify(),
        from_prod,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "db.schema.pre_table_suf"


def test_object_override():
    input_stringify = {
        "database_override": "over_db",
        "schema_override": "over_schema",
        "table_override": "over_table",
    }

    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(input_stringify),
        get_stringify(),
        from_prod,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "over_db.over_schema.over_table"


def test_object_from_prod_database_input_stringify():
    input_stringify = {"database_prefix": "pre", "database_suffix": "suf"}
    prod_sources = set(["db.schema.table"])
    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(),
        get_stringify(input_stringify),
        prod_sources,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "pre_db_suf.schema.table"


def test_object_from_prod_schema_input_stringify():
    input_stringify = {"schema_prefix": "pre", "schema_suffix": "suf"}
    prod_sources = set(["db.schema.table"])

    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(),
        get_stringify(input_stringify),
        prod_sources,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "db.pre_schema_suf.table"


def test_object_from_prod_table_input_stringify():
    input_stringify = {"table_prefix": "pre", "table_suffix": "suf"}
    prod_sources = set(["db.schema.table"])

    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(),
        get_stringify(input_stringify),
        prod_sources,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "db.schema.pre_table_suf"


def test_object_from_prod_override():
    input_stringify = {
        "database_override": "over_db",
        "schema_override": "over_schema",
        "table_override": "over_table",
    }
    prod_sources = set(["db.schema.table"])

    compiler = DbObjectCompiler(
        connetions,
        default_db,
        get_stringify(),
        get_stringify(input_stringify),
        prod_sources,
    )

    object = DbObject(compiler, "warehouse", "db", "schema", "table")

    stringify_object = object.compiler._common_value(object, False)

    assert stringify_object == "over_db.over_schema.over_table"


def test_object_from_string():
    compiler = DbObjectCompiler(
        connetions, default_db, get_stringify(), get_stringify(), from_prod
    )

    base_object = DbObject(compiler, "warehouse", "db", "schema", "table")

    object_string = "db.schema.table"
    compiled_object = compiler.from_string(object_string)

    assert base_object == compiled_object


def test_object_from_string_reference_level_schema():
    compiler = DbObjectCompiler(
        connetions, default_db, get_stringify(), get_stringify(), from_prod
    )

    base_object = DbObject(compiler, "warehouse", "db", "schema", None)

    compiled_object = compiler.from_string("db.schema", level="schema")
    assert base_object == compiled_object

    compiled_object = compiler.from_string("db.schema.")
    assert base_object == compiled_object

    base_object = DbObject(compiler, "warehouse", None, "schema", None)

    compiled_object = compiler.from_string("schema", level="schema")
    assert base_object == compiled_object

    compiled_object = compiler.from_string("schema.")
    assert base_object == compiled_object


def test_object_from_string_reference_level_database():
    compiler = DbObjectCompiler(
        connetions, default_db, get_stringify(), get_stringify(), from_prod
    )

    base_object = DbObject(compiler, "warehouse", "db", None, None)

    compiled_object = compiler.from_string("db", level="db")
    assert base_object == compiled_object

    compiled_object = compiler.from_string("db.", level="db")
    assert base_object == compiled_object

    compiled_object = compiler.from_string("db..")
    assert base_object == compiled_object


def test_object_from_string_level_error():
    compiler = DbObjectCompiler(
        connetions, default_db, get_stringify(), get_stringify(), from_prod
    )

    with pytest.raises(ValueError):
        compiler.from_string("test", level="something")

    with pytest.raises(ValueError):
        compiler.from_string("test", level="something.mambo:jambo")

    with pytest.raises(ValueError):
        compiler.from_string("test....")

    with pytest.raises(ValueError):
        compiler.from_string("test..", level="schema")

    with pytest.raises(ValueError):
        compiler.from_string("test.this..", level="schema")

    with pytest.raises(ValueError):
        compiler.from_string("test.this.that", level="schema")

    with pytest.raises(ValueError):
        compiler.from_string("test.this.that", level="db")
