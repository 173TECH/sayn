from contextlib import contextmanager

import pytest
from sayn.tasks.copy import CopyTask

from . import simulate_task, validate_table, tables_with_data, clear_tables


@contextmanager
def copy_task(source_db, target_db, source_data=None, target_data=None, **kwargs):
    task = CopyTask()
    simulate_task(task, source_db=source_db, target_db=target_db, **kwargs)
    if source_data is not None:
        with tables_with_data(task.connections["source_db"], source_data):
            if target_data is not None:
                with tables_with_data(task.connections["target_db"], target_data):
                    yield task
            else:
                yield task
    else:
        yield task

    if hasattr(task, "table"):
        clear_tables(
            task.connections["target_db"],
            [
                f"{task.schema +'.' if task.schema else ''}{task.table}",
                f"{task.tmp_schema +'.' if task.tmp_schema else ''}sayn_tmp_{task.table}",
            ],
        )


def test_copy_task(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


@pytest.mark.target_dbs(["sqlite", "postgresql", "mysql", "redshift", "snowflake"])
def test_copy_task_ddl(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
            ddl={"columns": [{"name": "x", "type": "int"}]},
        ).is_ok

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


@pytest.mark.target_dbs(["bigquery"])
def test_copy_task_ddl_bq(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
            ddl={"columns": [{"name": "x", "type": "int64"}]},
        ).is_ok

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


def test_copy_task_ddl_rename(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
            ddl={"columns": [{"name": "x", "dst_name": "y"}]},
        ).is_ok

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [{"y": 1}, {"y": 2}, {"y": 3}],
        )


def test_copy_task_error(source_db, target_db):
    """Testing copy task with config error src and dst instead of source and destination"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            src={"db": "source_db", "table": "source_table"},
            dst={"table": "dst_table"},
        ).is_err


def test_copy_task_incremental(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={
            "source_table": [
                {"id": 1, "name": "x"},
                {"id": 2, "name": "y"},
                {"id": 3, "name": "z"},
            ]
        },
        target_data={"dst_table": [{"id": 1, "name": "x"}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
            incremental_key="id",
            delete_key="id",
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [
                {"id": 1, "name": "x"},
                {"id": 2, "name": "y"},
                {"id": 3, "name": "z"},
            ],
        )


def test_copy_task_incremental2(source_db, target_db):
    """Testing copy task with no error"""
    with copy_task(
        source_db,
        target_db,
        source_data={
            "source_table": [
                {"id": 1, "updated_at": 2, "name": "x1"},
                {"id": 2, "updated_at": None, "name": "y"},
            ]
        },
        target_data={"dst_table": [{"id": 1, "updated_at": 1, "name": "x"}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"table": "dst_table"},
            incremental_key="updated_at",
            delete_key="id",
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [
                {"id": 1, "updated_at": 2, "name": "x1"},
                {"id": 2, "updated_at": None, "name": "y"},
            ],
        )


# test set db destination


def test_copy_task_dst_db(source_db, target_db):
    """Testing copy task with set db destination"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
        target_data={"dst_table": [{"id": 1, "updated_at": 1, "name": "x"}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"db": "target_db", "table": "dst_table"},
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok
        assert validate_table(
            task.connections["target_db"], "dst_table", [{"x": 1}, {"x": 2}, {"x": 3}]
        )


def test_copy_task_wrong_dst_db(source_db, target_db):
    """Testing copy task with wrong set db destination"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
        target_data={"dst_table": [{"id": 1, "updated_at": 1, "name": "x"}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"db": "wrong_db", "table": "dst_table"},
        ).is_err


# Testing schemas: this code expects 2 schemas in the target database: test and test2


@pytest.mark.source_dbs(["bigquery", "mysql", "postgresql", "redshift", "snowflake"])
@pytest.mark.target_dbs(["bigquery", "mysql", "postgresql", "redshift", "snowflake"])
def test_copy_schemas01(source_db, target_db):
    with copy_task(
        source_db,
        target_db,
        source_data={("test", "source_table"): [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "schema": "test", "table": "source_table"},
            destination={"table": "dst_table", "schema": "test2"},
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "test2.dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


@pytest.mark.source_dbs(["sqlite"])
def test_copy_schemas_error01(source_db, target_db):
    with copy_task(
        source_db,
        target_db,
    ) as task:
        assert task.setup(
            source={"db": "source_db", "schema": "test", "table": "source_table"},
            destination={"table": "dst_table", "schema": "test2"},
        ).is_err


@pytest.mark.source_dbs(["bigquery", "mysql", "postgresql", "redshift", "snowflake"])
@pytest.mark.target_dbs(["bigquery", "mysql", "postgresql", "redshift", "snowflake"])
def test_copy_schemas02(source_db, target_db):
    with copy_task(
        source_db,
        target_db,
        source_data={("test", "source_table"): [{"x": 1}, {"x": 2}, {"x": 3}]},
    ) as task:
        assert task.setup(
            source={"db": "source_db", "schema": "test", "table": "source_table"},
            destination={"table": "dst_table", "schema": "test2", "tmp_schema": "test"},
        ).is_ok

        task.source_db._introspect()
        task.target_db._introspect()

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "test2.dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


@pytest.mark.target_dbs(["sqlite"])
def test_copy_schemas_error02(source_db, target_db):
    with copy_task(
        source_db,
        target_db,
    ) as task:
        assert task.setup(
            source={"db": "source_db", "schema": "test", "table": "source_table"},
            destination={"table": "dst_table", "schema": "test2", "tmp_schema": "test"},
        ).is_err
