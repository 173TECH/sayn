from . import simulate_task, validate_table


def test_copy_task():
    # testing copy task with no error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"table": "dst_table"},
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    assert validate_table(task.default_db, "dst_table", [{"x": 1}, {"x": 2}, {"x": 3}],)


def test_copy_task_ddl():
    # testing copy task with no error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"table": "dst_table"},
        ddl={"columns": [{"name": "x", "type": "int"}]},
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    assert validate_table(task.default_db, "dst_table", [{"x": 1}, {"x": 2}, {"x": 3}],)


def test_copy_task_error():
    # testing copy task with config error src and dst instead of source and destination
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        src={"db": "source_db", "table": "source_table"}, dst={"table": "dst_table"},
    )
    assert setup_result.is_err


def test_copy_task_incremental():
    # testing copy task with no error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        'CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS id, CAST("x" AS TEXT) AS name UNION SELECT 2 AS id, "y" AS name UNION SELECT 3 AS id, "z" AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # create table destination with one value
    destination_db = task.connections["target_db"]
    destination_db.execute(
        'CREATE TABLE dst_table AS SELECT CAST(1 AS INTEGER) AS id, CAST("x" AS TEXT) AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"table": "dst_table"},
        incremental_key="id",
        delete_key="id",
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    assert validate_table(
        task.default_db,
        "dst_table",
        [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}, {"id": 3, "name": "z"}],
    )


def test_copy_task_incremental2():
    # testing copy task with no error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        'CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS id, CAST(2 AS INTEGER) AS updated_at, CAST("x1" AS TEXT) AS name UNION SELECT 2 AS id, NULL AS updated_at, "y" AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # create table destination with one value
    destination_db = task.connections["target_db"]
    destination_db.execute(
        'CREATE TABLE dst_table AS SELECT CAST(1 AS INTEGER) AS id, CAST(1 AS INTEGER) AS updated_at, CAST("x" AS TEXT) AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"table": "dst_table"},
        incremental_key="updated_at",
        delete_key="id",
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    assert validate_table(
        task.default_db,
        "dst_table",
        [
            {"id": 1, "updated_at": 2, "name": "x1"},
            {"id": 2, "updated_at": None, "name": "y"},
        ],
    )


# test set db destination


def test_copy_task_dst_db():
    # testing copy task with set db destination
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"db": "target_db2", "table": "dst_table"},
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    target_db = task.connections["target_db2"]
    assert validate_table(target_db, "dst_table", [{"x": 1}, {"x": 2}, {"x": 3}],)


def test_copy_task_wrong_dst_db():
    # testing copy task with wrong set db destination
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"db": "wrong_db", "table": "dst_table"},
    )
    assert setup_result.is_err
