import json
import pytest

from sayn.app.config import read_settings, ConfigError

from . import create_project


def test_correct_01(tmpdir):
    settings_yaml = """default_profile: dev

profiles:
  dev:
    credentials:
      warehouse: dev_db
  prod:
    credentials:
      warehouse: prod_db

credentials:
  dev_db:
    type: sqlite
    database: dev.db
  prod_db:
    type: sqlite
    database: prod.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        settings = read_settings()
        assert settings.get_settings() == {
            "credentials": {"warehouse": {"type": "sqlite", "database": "dev.db"}},
            "parameters": None,
        }


def test_correct_02(tmpdir):
    settings_yaml = """
profiles:
  dev:
    credentials:
      warehouse: dev_db

credentials:
  dev_db:
    type: sqlite
    database: dev.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        settings = read_settings()
        assert settings.get_settings() == {
            "credentials": {"warehouse": {"type": "sqlite", "database": "dev.db"}},
            "parameters": None,
        }


def test_error_profile01(tmpdir):
    settings_yaml = """

profiles:
  dev:
    credentials:
      warehouse: dev_db
  prod:
    credentials:
      warehouse: prod_db

credentials:
  dev_db:
    type: sqlite
    database: dev.db
  prod_db:
    type: sqlite
    database: prod.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        with pytest.raises(ConfigError):
            read_settings()


def test_error_profile02(tmpdir):
    settings_yaml = """default_profile: not_a_correct_profile

profiles:
  dev:
    credentials:
      warehouse: dev_db
  prod:
    credentials:
      warehouse: prod_db

credentials:
  dev_db:
    type: sqlite
    database: dev.db
  prod_db:
    type: sqlite
    database: prod.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        with pytest.raises(ConfigError):
            read_settings()


def test_error_consistency_01(tmpdir):
    settings_yaml = """
profiles:
  dev:
    credentials:
      warehouse: dev_db

credentials:
  other_db:
    type: sqlite
    database: dev.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        with pytest.raises(ConfigError):
            read_settings()


def test_missing_props01(tmpdir):
    settings_yaml = """
credentials:
  other_db:
    type: sqlite
    database: dev.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        with pytest.raises(ConfigError):
            read_settings()


def test_missing_props02(tmpdir):
    settings_yaml = """
profiles:
  dev:
    credentials:
      warehouse: dev_db
"""

    with create_project(tmpdir, settings=settings_yaml):
        with pytest.raises(ConfigError):
            read_settings()


def test_env_01(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": "value1",
        "SAYN_CREDENTIAL_cred1": json.dumps({"type": "sqlite", "database": "test.db"}),
    }

    with create_project(tmpdir, env=env):
        settings = read_settings()
        assert settings.get_settings() == {
            "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
            "parameters": {"param1": "value1"},
        }
