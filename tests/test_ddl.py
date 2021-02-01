from sayn.database.creator import create as create_db


def validate_ddl(ddl):
    db = create_db("test", "test", {"type": "sqlite", "database": ":memory:"})
    return db._validate_ddl(ddl)


def test_ddl_empty():
    result = validate_ddl({})
    assert result.is_ok and result.value == {
        "columns": [],
        "indexes": {},
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_cols01():
    result = validate_ddl({"columns": ["col1"]})
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_cols02():
    result = validate_ddl({"columns": ["col1", {"name": "col2"}]})
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
            {
                "name": "col2",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_cols03():
    result = validate_ddl({"columns": ["col1", {"name": "col2", "type": "BIGINT"}]})
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "primary": False,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_cols04():
    result = validate_ddl(
        {"columns": ["col1", {"name": "col2", "type": "BIGINT", "primary": True}]}
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "primary": True,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": ["col2"],
        "permissions": {},
    }


def test_ddl_cols05():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "not_null": True},
                {"name": "col2", "type": "BIGINT", "primary": True},
            ]
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": True,
                "unique": False,
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "primary": True,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": ["col2"],
        "permissions": {},
    }


def test_ddl_cols06():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "not_null": True, "primary": True},
                {"name": "col2", "type": "BIGINT", "primary": True},
            ]
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": True,
                "not_null": True,
                "unique": False,
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "primary": True,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": ["col1", "col2"],
        "permissions": {},
    }


def test_ddl_cols07():
    result = validate_ddl({"columns": ["dupe_col", "dupe_col"]})
    assert result.is_err


def test_ddl_cols08():
    result = validate_ddl({"columns": ["dupe_col", {"name": "dupe_col"}]})
    assert result.is_err


def test_ddl_idx01():
    result = validate_ddl({"indexes": {"idx1": {"columns": ["col1", "col2"]}}})
    assert result.is_ok and result.value == {
        "columns": [],
        "indexes": {"idx1": {"columns": ["col1", "col2"]}},
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_idx02():
    result = validate_ddl(
        {
            "indexes": {
                "idx1": {"columns": ["col1", "col2"]},
                "idx2": {"columns": ["col1", "col2"]},
            }
        }
    )
    assert result.is_ok and result.value == {
        "columns": [],
        "indexes": {
            "idx1": {"columns": ["col1", "col2"]},
            "idx2": {"columns": ["col1", "col2"]},
        },
        "primary_key": [],
        "permissions": {},
    }


def test_ddl_pk01():
    result = validate_ddl(
        {
            "columns": ["col1", "col2"],
            "indexes": {"primary_key": {"columns": ["col1", "col2"]}},
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
            {
                "name": "col2",
                "type": None,
                "primary": False,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": ["col1", "col2"],
        "permissions": {},
    }


def test_ddl_pk02():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "primary": True},
                {"name": "col2", "primary": True},
            ],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                "primary": True,
                "not_null": False,
                "unique": False,
            },
            {
                "name": "col2",
                "type": None,
                "primary": True,
                "not_null": False,
                "unique": False,
            },
        ],
        "indexes": {},
        "primary_key": ["col1", "col2"],
        "permissions": {},
    }


def test_ddl_pk03():
    result = validate_ddl({"indexes": {"primary_key": {"columns": ["col1", "col2"]}}})
    assert result.is_ok and result.value == {
        "columns": [],
        "indexes": {},
        "primary_key": ["col1", "col2"],
        "permissions": {},
    }


def test_ddl_pk04():
    result = validate_ddl({"indexes": {"primary_key": {"columns": []}}})
    assert result.is_err


def test_ddl_pk05():
    result = validate_ddl(
        {"columns": ["col1"], "indexes": {"primary_key": {"columns": ["col1", "col2"]}}}
    )
    assert result.is_err
