from contextlib import contextmanager
import os
import tempfile
from pathlib import Path
import subprocess
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from sayn.database.creator import create as create_db
from sayn.tasks.sql import SqlTask
from sayn.tasks.autosql import AutoSqlTask


@contextmanager
def inside_dir(dirpath):
    """
    Execute code from inside the given directory
    :param dirpath: String, path of the directory the command is being run.
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        yield
    finally:
        os.chdir(old_path)


@contextmanager
def create_project(dirpath, settings=None, project=None, dags=dict(), env=dict()):
    """
    Execute code from inside the given directory, creating the sayn project files
    :param settings: String, yaml for a settings.yaml file
    :param project: String, yaml for a project.yaml file
    :param dags: Dict, dict of yaml for the contents of the dags folder
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        if settings is not None:
            Path(dirpath, "settings.yaml").write_text(settings)
        if project is not None:
            Path(dirpath, "project.yaml").write_text(project)
        if len(dags) > 0:
            for name, dag in dags.items():
                Path(dirpath, f"{name}.yaml").write_text(dag)
        if len(env) > 0:
            os.environ.update(env)
        yield
    finally:
        os.chdir(old_path)
        for k in env.keys():
            del os.environ[k]


def run_sayn(*args):
    return subprocess.check_output(
        f"sayn {' '.join(args)}", shell=True, stderr=subprocess.STDOUT
    )


# Task Simulators

# create empty tracker class to enable the run to go through
class VoidTracker:
    def set_run_steps(self, steps):
        pass

    def start_step(self, step):
        pass

    def finish_current_step(self):
        pass


vd = VoidTracker()


def simulate_task(type):
    if type == "sql":
        task = SqlTask()
    elif type == "autosql":
        task = AutoSqlTask()
    else:
        pass

    task.name = "test_task"  # set for compilation output during run
    task.dag = "test_dag"  # set for compilation output during run
    task.run_arguments = {
        "folders": {"sql": "sql", "compile": "compile"},
        "command": "run",
        "debug": False,
        "full_load": False,
    }
    task.connections = {
        "test_db": create_db(
            "test_db", "test_db", {"type": "sqlite", "database": ":memory:"}
        )
    }
    task._default_db = "test_db"
    task.tracker = vd

    task.jinja_env = Environment(
        loader=FileSystemLoader(os.getcwd()),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )

    return task


def simulate_task_setup(task, **kwargs):
    if isinstance(task, (SqlTask, AutoSqlTask)):
        tmp_path = tempfile.mkdtemp("tmp_test")
        with inside_dir(str(tmp_path)):
            fpath = Path(str(tmp_path), "sql", "test.sql")
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(kwargs["sql_query"])
            if isinstance(task, SqlTask):
                setup_result = task.setup("test.sql")
            if isinstance(task, AutoSqlTask):
                setup_result = task.setup(**kwargs["task_config"])

    return setup_result


def simulate_task_setup_run(task, **kwargs):
    setup_result = simulate_task_setup(task, **kwargs)
    run_result = task.run()
    return setup_result, run_result
