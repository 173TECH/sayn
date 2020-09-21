from pathlib import Path


import pytest

from . import inside_dir, run_sayn


project_name = "project01"


@pytest.fixture(scope="module")
def tmp_root_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("project01")
    with inside_dir(str(tmp_path)):
        run_sayn("init", project_name)

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
        run_sayn("run")

        assert Path("dev.db").exists()


def test_sayn_run_prod(tmp_root_path):
    with inside_dir(str(tmp_root_path / project_name)):
        run_sayn("run", "-p", "prod")

        assert Path("prod.db").exists()
