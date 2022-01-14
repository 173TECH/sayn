from contextlib import contextmanager
import os
from pathlib import Path
import subprocess

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from ruamel.yaml import YAML

from sayn.database.creator import create as create_db
from sayn.tasks.task_wrapper import TaskWrapper
from sayn.utils.compiler import Compiler


@contextmanager
def inside_dir(dirpath, fs=dict()):
    """
    Execute code from inside the given directory
    :param dirpath: String, path of the directory the command is being run.
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        for filepath, content in fs.items():
            fpath = Path(filepath)
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content)
        yield
    finally:
        os.chdir(old_path)


@contextmanager
def create_project(dirpath, settings=None, project=None, groups=dict(), env=dict()):
    """
    Execute code from inside the given directory, creating the sayn project files
    :param settings: String, yaml for a settings.yaml file
    :param project: String, yaml for a project.yaml file
    :param groups: Dict, dict of yaml for the contents of the tasks folder
    """
    old_path = os.getcwd()
    try:
        os.chdir(dirpath)
        if settings is not None:
            Path(dirpath, "settings.yaml").write_text(settings)
        if project is not None:
            Path(dirpath, "project.yaml").write_text(project)
        if len(groups) > 0:
            for name, group in groups.items():
                Path(dirpath, f"{name}.yaml").write_text(group)
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

    def info(self, msg):
        pass


vd = VoidTracker()


def simulate_task(
    task_class,
    task_type,
    source_db=None,
    target_db=None,
    run_arguments=dict(),
    task_params=dict(),
):

    connections = dict()
    if target_db is not None:
        connections = {
            "target_db": create_db(
                "target_db", "target_db", target_db.copy(), dict(), dict()
            )
        }

    if source_db is not None:
        connections.update(
            {
                "source_db": create_db(
                    "source_db", "source_db", source_db.copy(), dict(), dict()
                )
            }
        )

    run_arguments = {
        "folders": {"sql": "sql", "compile": "compile", "tests": "tests"},
        "command": "run",
        "debug": False,
        "full_load": False,
        **run_arguments,
    }

    wrapper = TaskWrapper(
        "test_group",
        "test_task",
        task_type,
        None,  # on_fail
        set(),  # parents
        set(),  # sources
        set(),  # outputs
        set(),  # tags
        vd,
        task_class,
        connections,
        "target_db",
        run_arguments,
        Compiler(run_arguments, dict(), task_params),
    )

    return wrapper.runner


def validate_table(db, table_name, expected_data, variable_columns=None):
    result = db.read_data(f"select * from {table_name}")
    if len(result) != len(expected_data):
        return False

    result = sorted(result, key=lambda x: list(x.values()))
    expected_data = sorted(expected_data, key=lambda x: list(x.values()))
    for i in range(len(result)):
        if variable_columns is None:
            if result[i] != expected_data[i]:
                return False
        else:
            expected_clean = {
                k: v for k, v in expected_data[i].items() if k not in variable_columns
            }
            result_clean = {
                k: v for k, v in result[i].items() if k not in variable_columns
            }

            if expected_clean != result_clean:
                return False

            if result[i].keys() != expected_data[i].keys():
                return False

    return True


@contextmanager
def tables_with_data(db, tables, extra_tables=list()):
    tables_to_delete = extra_tables.copy()
    for table, data in tables.items():
        if isinstance(table, tuple):
            schema = table[0]
            table = table[1]
            tables_to_delete.append(f"{schema}.{table}")
        else:
            schema = None
            tables_to_delete.append(table)

        db.load_data(table, data, schema=schema, replace=True)

    try:
        yield
    finally:
        clear_tables(db, tables_to_delete)


def clear_tables(db, tables):
    for table in tables:
        try:
            db.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            pass

        try:
            db.execute(f"DROP VIEW IF EXISTS {table}")
        except:
            pass
