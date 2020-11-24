from . import inside_dir, simulate_task, validate_table

sql_query = "CREATE TABLE test_sql_task AS SELECT 1 AS x"
sql_query_param = "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x"
sql_query_err = "SELECT * FROM non_existing_table"
sql_query_multi = (
    "CREATE TABLE test_t1 AS SELECT 1 AS x; CREATE TABLE test_t2 AS SELECT 2 AS x;"
)


def test_sql_task(tmp_path):
    # test correct setup and run based for correct sql
    with inside_dir(tmp_path):
        task = simulate_task("sql", sql_query=sql_query)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query", "Execute Query"]

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_sql_task", [{"x": 1}],)


def test_sql_task_compile(tmp_path):
    # test correct setup and compile for correct sql
    with inside_dir(tmp_path):
        task = simulate_task(
            "sql", sql_query=sql_query, run_arguments={"command": "compile"}
        )

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query"]

        # compile
        compile_result = task.compile()
        assert compile_result.is_ok


def test_sql_task_param(tmp_path):
    # test correct setup and run for correct sql with parameter
    with inside_dir(tmp_path):
        task = simulate_task(
            "sql", sql_query=sql_query_param, task_params={"user_prefix": "tu_"}
        )

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == "CREATE TABLE tu_test_sql_task AS SELECT 1 AS x"

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(task.default_db, "tu_test_sql_task", [{"x": 1}],)


def test_sql_task_param_err(tmp_path):
    # test setup error for correct sql but missing parameter
    with inside_dir(tmp_path):
        task = simulate_task("sql", sql_query=sql_query_param)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_err


def test_sql_task_run_err(tmp_path):
    # test correct setup and run error for incorrect sql
    with inside_dir(tmp_path):
        task = simulate_task("sql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok

        # run
        run_result = task.run()
        assert run_result.is_err


def test_sql_task_run_multi_statements(tmp_path):
    # test correct setup and run for multiple sql statements
    with inside_dir(tmp_path):
        task = simulate_task("sql", sql_query=sql_query_multi)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == sql_query_multi
        assert task.steps == ["Write Query", "Execute Query"]

        # run
        run_result = task.run()
        assert run_result.is_ok
        assert validate_table(task.default_db, "test_t1", [{"x": 1}],)
        assert validate_table(task.default_db, "test_t2", [{"x": 2}],)
