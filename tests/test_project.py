from pathlib import Path

from sayn.core.config import read_project
from . import inside_dir

# utils


def setup_project_and_tasks(project_yaml=None, base_yaml=None):
    # create the project.yaml file
    if project_yaml is not None:
        fpath1 = Path("project.yaml")
        fpath1.write_text(project_yaml)

    # create the tasks folder and base task group
    if base_yaml is not None:
        fpath2 = Path("tasks", "base.yaml")
        fpath2.parent.mkdir(parents=True, exist_ok=True)
        fpath2.write_text(base_yaml)
    else:
        Path("tasks").mkdir(parents=True, exist_ok=True)


# tests


def test_project(tmp_path):
    # test ok setup
    project_yaml = """
    required_credentials:
      - warehouse

    default_db: warehouse
    """

    base_yaml = """
    tasks:
        test_sql:
            type: sql
            file_name: test.sql
    """

    with inside_dir(tmp_path):
        setup_project_and_tasks(project_yaml=project_yaml, base_yaml=base_yaml)
        assert read_project().is_ok


def test_project_err_no_tasks(tmp_path):
    # test error if no task group in tasks folder
    project_yaml = """
    required_credentials:
      - warehouse

    default_db: warehouse
    """

    with inside_dir(tmp_path):
        setup_project_and_tasks(project_yaml=project_yaml)
        assert read_project().is_err
