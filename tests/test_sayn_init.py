from contextlib import contextmanager
import os
from pathlib import Path

from click.testing import CliRunner
import pytest

from sayn.commands.sayn_powers import cli


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


project_name = "project01"


@pytest.fixture(scope="module")
def tmp_root_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("project01")
    with inside_dir(str(tmp_path)):
        runner = CliRunner()
        response = runner.invoke(cli, ["init", project_name])

    assert response.exit_code == 0

    return tmp_path


def test_sayn_init_contents(tmp_root_path):
    with inside_dir(str(tmp_root_path / project_name)):
        assert Path("settings.yaml").exists()
        assert Path("project.yaml").exists()
        assert Path("dags", "base.yaml").exists()
        assert not Path("test.db").exists()
        assert not Path("prod.db").exists()


def test_sayn_run_default(tmp_root_path):
    with inside_dir(str(tmp_root_path / project_name)):
        runner = CliRunner()
        response = runner.invoke(cli, ["run"])

        assert response.exit_code == 0

        assert Path("dev.db").exists()


def test_sayn_run_prod(tmp_root_path):
    with inside_dir(str(tmp_root_path / project_name)):
        runner = CliRunner()
        response = runner.invoke(cli, ["run", "-p", "prod"])

        assert response.exit_code == 0

        assert Path("prod.db").exists()
