import json

from sayn.core.settings import read_settings, get_settings

from . import create_project


def sort_obj(val):
    if isinstance(val, dict):
        out = dict()
        for k in sorted(val.keys()):
            out[k] = sort_obj(val[k])
        return out
    elif isinstance(val, list):
        out = list()
        for v in val:
            out.append(v)
        return out
    else:
        return val


def get_settings_dict():
    settings = read_settings().value
    settings = get_settings(settings["yaml"], settings["env"], None).value

    return settings


def equal_objs(d1, d2):
    return sort_obj(d1) == sort_obj(d2)


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
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"warehouse": {"type": "sqlite", "database": "dev.db"}},
                "parameters": None,
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


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
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"warehouse": {"type": "sqlite", "database": "dev.db"}},
                "parameters": None,
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


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
        assert read_settings().is_err


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
        assert read_settings().is_err


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
        assert read_settings().is_err


def test_missing_props01(tmpdir):
    settings_yaml = """
credentials:
  other_db:
    type: sqlite
    database: dev.db
"""

    with create_project(tmpdir, settings=settings_yaml):
        assert read_settings().is_err


def test_missing_props02(tmpdir):
    settings_yaml = """
profiles:
  dev:
    credentials:
      warehouse: dev_db
"""

    with create_project(tmpdir, settings=settings_yaml):
        assert read_settings().is_err


def test_env_01(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": "value1",
        "SAYN_CREDENTIAL_cred1": json.dumps({"type": "sqlite", "database": "test.db"}),
    }

    with create_project(tmpdir, env=env):
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
                "parameters": {"param1": "value1"},
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


def test_env_02(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": "value1",
        "SAYN_CREDENTIAL_cred1": "type: sqlite\ndatabase: test.db",
    }

    with create_project(tmpdir, env=env):
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
                "parameters": {"param1": "value1"},
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


def test_env_03(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": '["value1", "value2"]',
        "SAYN_CREDENTIAL_cred1": "type: sqlite\ndatabase: test.db",
    }

    with create_project(tmpdir, env=env):
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
                "parameters": {"param1": ["value1", "value2"]},
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


def test_env_04(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": "1",
        "SAYN_CREDENTIAL_cred1": "type: sqlite\ndatabase: test.db",
    }

    with create_project(tmpdir, env=env):
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
                "parameters": {"param1": 1},
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )


def test_env_05(tmpdir):
    env = {
        "SAYN_PARAMETER_param1": "key1: value1\nkey2: value2\nkey3:\n  - value3\n  - value4",
        "SAYN_CREDENTIAL_cred1": json.dumps({"type": "sqlite", "database": "test.db"}),
    }

    with create_project(tmpdir, env=env):
        settings = get_settings_dict()
        assert equal_objs(
            settings,
            {
                "credentials": {"cred1": {"type": "sqlite", "database": "test.db"}},
                "parameters": {
                    "param1": {
                        "key1": "value1",
                        "key2": "value2",
                        "key3": ["value3", "value4"],
                    }
                },
                "stringify": dict(),
                "from_prod": list(),
                "default_run": {
                    "include": set(),
                    "exclude": set(),
                    "upstream_prod": None,
                },
            },
        )
