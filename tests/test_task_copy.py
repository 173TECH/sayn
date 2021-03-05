from contextlib import contextmanager

from sayn.tasks.copy import CopyTask

from . import (
    simulate_task,
    validate_table,
    tables_with_data,
    clear_tables,
    pytest_generate_tests,
)


@contextmanager
def copy_task(
    source_db,
    target_db,
    source_data=None,
    target_data=None,
    clear_target=True,
    **kwargs
):
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

    if clear_target:
        clear_tables(task.connections["target_db"], [task.table])


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

        assert task.run().is_ok

        assert validate_table(
            task.default_db,
            "dst_table",
            [{"x": 1}, {"x": 2}, {"x": 3}],
        )


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


def test_copy_task_error(source_db, target_db):
    """Testing copy task with config error src and dst instead of source and destination"""
    with copy_task(
        source_db,
        target_db,
        source_data={"source_table": [{"x": 1}, {"x": 2}, {"x": 3}]},
        clear_target=False,
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
    ) as task:
        dst_data = {"dst_table": [{"id": 1, "name": "x"}]}
        with tables_with_data(task.connections["target_db"], dst_data):
            assert task.setup(
                source={"db": "source_db", "table": "source_table"},
                destination={"table": "dst_table"},
                incremental_key="id",
                delete_key="id",
            ).is_ok

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
        clear_target=False,
    ) as task:
        assert task.setup(
            source={"db": "source_db", "table": "source_table"},
            destination={"db": "wrong_db", "table": "dst_table"},
        ).is_err
