from . import simulate_task


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
        destination={"db": "test_db", "table": "dst_table"},
    )
    assert setup_result.is_ok
    assert task.steps == [
        "Cleanup",
        "Create Temp DDL",
        "Load Data",
        "Cleanup Target",
        "Move",
    ]

    # run
    run_result = task.run()
    assert run_result.is_ok
    task_result = task.connections["test_db"].select("SELECT * FROM dst_table")
    assert len(task_result) == 3
    assert task_result[0]["x"] == 1
    assert task_result[2]["x"] == 3


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
        destination={"db": "test_db", "table": "dst_table"},
        ddl={"columns": [{"name": "x", "type": "int"}]},
    )
    assert setup_result.is_ok
    assert task.steps == [
        "Cleanup",
        "Create Temp DDL",
        "Load Data",
        "Cleanup Target",
        "Move",
    ]

    # run
    run_result = task.run()
    assert run_result.is_ok
    task_result = task.connections["test_db"].select("SELECT * FROM dst_table")
    assert len(task_result) == 3
    assert task_result[0]["x"] == 1
    assert task_result[2]["x"] == 3


def test_copy_task_error():
    # testing copy task with config error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        "CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS x UNION SELECT 2 AS x UNION SELECT 3 AS x"
    )  # for integer cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        src={"db": "source_db", "table": "source_table"},
        dst={"db": "test_db", "table": "dst_table"},
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
    destination_db = task.connections["test_db"]
    destination_db.execute(
        'CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS id, CAST("x" AS TEXT) AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"db": "test_db", "table": "dst_table"},
        incremental_key="id",
        delete_key="id",
    )
    assert setup_result.is_ok
    assert task.steps == ["Cleanup", "Create Temp DDL", "Load Data", "Merge"]

    # run
    run_result = task.run()
    assert run_result.is_ok
    task_result = task.connections["test_db"].select("SELECT * FROM dst_table")
    assert len(task_result) == 3
    assert task_result[0]["id"] == 1
    assert task_result[0]["name"] == "x"
    assert task_result[2]["name"] == "z"


def test_copy_task_incremental2():
    # testing copy task with no error
    task = simulate_task("copy")

    # create table to transfer
    source_db = task.connections["source_db"]
    source_db.execute(
        'CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS id, CAST(2 AS INTEGER) AS updated_at, CAST("x1" AS TEXT) AS name UNION SELECT 2 AS id, 2 AS updated_at, "y" AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # create table destination with one value
    destination_db = task.connections["test_db"]
    destination_db.execute(
        'CREATE TABLE source_table AS SELECT CAST(1 AS INTEGER) AS id, CAST(1 AS INTEGER) AS updated_at, CAST("x" AS TEXT) AS name'
    )  # cast otherwise SQLite sets the type as NULL

    # setup
    setup_result = task.setup(
        source={"db": "source_db", "table": "source_table"},
        destination={"db": "test_db", "table": "dst_table"},
        incremental_key="updated_at",
        delete_key="id",
    )
    assert setup_result.is_ok

    # run
    run_result = task.run()
    assert run_result.is_ok
    task_result = task.connections["test_db"].select("SELECT * FROM dst_table")
    assert len(task_result) == 2
    assert task_result[0]["id"] == 1
    assert task_result[0]["name"] == "x1"
    assert task_result[1]["name"] == "y"
