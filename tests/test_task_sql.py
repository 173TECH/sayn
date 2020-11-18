from . import inside_dir, simulate_task

sql_query = "CREATE TABLE test_sql_task AS SELECT 1 AS x"
sql_query_param = "CREATE TABLE {{user_prefix}}test_sql_task AS SELECT 1 AS x"
sql_query_err = "SELECT * FROM non_existing_table"
sql_query_multi = (
    "CREATE TABLE test_t1 AS SELECT 1 AS x; CREATE TABLE test_t2 AS SELECT 2 AS x;"
)


def test_sql_task(tmp_path):
    # test correct setup and run based for correct sql
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == sql_query
        assert task.steps == ["Write Query", "Execute Query"]

        # run
        run_result = task.run()
        assert run_result.is_ok

        task_result = task.default_db.select("SELECT * FROM test_sql_task")
        assert task_result[0]["x"] == 1


def test_sql_task_compile(tmp_path):
    # test correct setup and compile for correct sql
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query)
        task.run_arguments.update({"command": "compile"})

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
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_param)
        task.task_parameters = {"user_prefix": "tu_"}
        task.jinja_env.globals.update(**task.task_parameters)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == "CREATE TABLE tu_test_sql_task AS SELECT 1 AS x"

        # run
        run_result = task.run()
        assert run_result.is_ok

        task_result = task.default_db.select("SELECT * FROM tu_test_sql_task")
        assert task_result[0]["x"] == 1


def test_sql_task_param_err(tmp_path):
    # test setup error for correct sql but missing parameter
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_param)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_err


def test_sql_task_run_err(tmp_path):
    # test correct setup and run error for incorrect sql
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_err)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok

        # run
        run_result = task.run()
        assert run_result.is_err


def test_sql_task_run_multi_statements(tmp_path):
    # test correct setup and run for multiple sql statements
    with inside_dir(str(tmp_path)):
        task = simulate_task("sql", sql_query=sql_query_multi)

        # setup
        setup_result = task.setup("test.sql")
        assert setup_result.is_ok
        assert task.sql_query == sql_query_multi
        assert task.steps == ["Write Query", "Execute Query"]

        # run
        run_result = task.run()
        assert run_result.is_ok

        task_result1 = task.default_db.select("SELECT * FROM test_t1")
        task_result2 = task.default_db.select("SELECT * FROM test_t2")
        assert task_result1[0]["x"] == 1
        assert task_result2[0]["x"] == 2
