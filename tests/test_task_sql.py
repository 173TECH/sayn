from contextlib import contextmanager

import pytest

from sayn.tasks.sql import SqlTask

from . import inside_dir, simulate_task, validate_table, tables_with_data, clear_tables


@contextmanager
def sql_task(tmp_path, target_db, sql, data=None, drop_tables=list(), **kwargs):
    fs = {"sql/test.sql": sql} if sql is not None else dict()
    with inside_dir(tmp_path, fs):
        task = simulate_task(SqlTask, dict(), target_db=target_db, **kwargs)
        if data is not None:
            with tables_with_data(task.connections["target_db"], data):
                yield task
        else:
            yield task

        if len(drop_tables) > 0:
            clear_tables(task.connections["target_db"], drop_tables)


def test_sql_task(tmp_path, target_db):
    """Test correct setup and run based for correct sql"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE test_sql_task AS SELECT 1 AS x",
        drop_tables=["test_sql_task"],
    ) as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.run().is_ok
        assert validate_table(task.default_db, "test_sql_task", [{"x": 1}])


def test_sql_task_compile(tmp_path, target_db):
    """Test correct setup and compile for correct sql"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE test_sql_task AS SELECT 1 AS x",
        drop_tables=["test_sql_task"],
        run_arguments={"command": "compile"},
    ) as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.compile().is_ok


def test_sql_task_param(tmp_path, target_db):
    """Test correct setup and run for correct sql with parameter"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x",
        drop_tables=["tu_test_sql_task"],
        task_params={"user_prefix": "tu_"},
    ) as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.run().is_ok
        assert validate_table(
            task.default_db,
            "tu_test_sql_task",
            [{"x": 1}],
        )


def test_sql_task_param_err(tmp_path, target_db):
    """Test setup error for correct sql but missing parameter"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x",
    ) as task:
        with pytest.raises(Exception):
            task.config(file_name="test.sql")


def test_sql_task_run_err(tmp_path, target_db):
    """Test correct setup and run error for incorrect sql"""
    with sql_task(tmp_path, target_db, "SELECT * FROM non_existing_table") as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.run().is_err


def test_sql_task_run_multi_statements(tmp_path, target_db):
    """Test correct setup and run for multiple sql statements"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE test_t1 AS SELECT 1 AS x; CREATE TABLE test_t2 AS SELECT 2 AS x;",
        drop_tables=["test_t1", "test_t2"],
    ) as task:
        assert task.config(file_name="test.sql").is_ok
        assert task.setup(False).is_ok

        assert task.run().is_ok
        assert validate_table(
            task.default_db,
            "test_t1",
            [{"x": 1}],
        )
        assert validate_table(
            task.default_db,
            "test_t2",
            [{"x": 2}],
        )


# test set db destination


def test_sql_task_dst_db(tmp_path, target_db):
    """Test correct setup and run based for correct sql"""
    with sql_task(
        tmp_path,
        target_db,
        "CREATE TABLE test_sql_task AS SELECT 1 AS x",
        drop_tables=["test_sql_task"],
    ) as task:
        assert task.config(file_name="test.sql", db="target_db").is_ok
        assert task.setup(False).is_ok

        assert task.run().is_ok
        assert validate_table(
            task.target_db,
            "test_sql_task",
            [{"x": 1}],
        )


def test_sql_task_wrong_dst_db(tmp_path, target_db):
    """Test correct setup and run based for correct sql"""
    with sql_task(
        tmp_path, target_db, "CREATE TABLE test_sql_task AS SELECT 1 AS x"
    ) as task:
        assert task.config(file_name="test.sql", db="wrong_db").is_err
