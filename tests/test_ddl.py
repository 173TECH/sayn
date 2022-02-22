from sayn.database.creator import create as create_db


def validate_ddl(ddl):
    db = create_db(
        "test",
        "test",
        {"type": "sqlite", "database": ":memory:"},
    )

    res = db._validate_ddl(ddl["columns"], ddl["table_properties"], ddl["post_hook"])

    if res.is_err:
        return res

    return db._format_properties(res.value)


def test_ddl_empty():
    result = validate_ddl({"columns": [], "table_properties": {}, "post_hook": []})
    assert result.is_ok and result.value == {
        "columns": [],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols01():
    result = validate_ddl(
        {
            "columns": [{"name": "col1", "tests": []}],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "dst_name": None,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols02():
    result = validate_ddl(
        {
            "columns": [{"name": "col1", "tests": []}, {"name": "col2", "tests": []}],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
            {
                "name": "col2",
                "type": None,
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols03():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "tests": []},
                {"name": "col2", "type": "BIGINT", "tests": []},
            ],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols04():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "tests": []},
                {"name": "col2", "type": "BIGINT", "tests": ["unique"]},
            ],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                # "primary": True,
                "not_null": False,
                "unique": True,
                "dst_name": None,
                "values": False,
                "tests": [{"type": "unique", "values": [], "execute": True}],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols05():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "tests": ["not_null"]},
                {"name": "col2", "type": "BIGINT", "tests": ["unique"]},
            ],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "primary": False,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [{"type": "not_null", "values": [], "execute": True}],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                # "primary": True,
                "not_null": False,
                "unique": True,
                "dst_name": None,
                "values": False,
                "tests": [{"type": "unique", "values": [], "execute": True}],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols06():
    result = validate_ddl(
        {
            "columns": [
                {"name": "col1", "tests": ["not_null", "unique"]},
                {"name": "col2", "type": "BIGINT", "tests": [{"name": "not_null"}]},
            ],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_ok and result.value == {
        "columns": [
            {
                "name": "col1",
                "type": None,
                # "primary": True,
                "not_null": False,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [
                    {"type": "not_null", "values": [], "execute": True},
                    {"type": "unique", "values": [], "execute": True},
                ],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                # "primary": True,
                "not_null": True,
                "unique": False,
                "dst_name": None,
                "values": False,
                "tests": [{"type": "not_null", "values": [], "execute": True}],
            },
        ],
        "properties": [],
        "post_hook": [],
    }


def test_ddl_cols07():
    result = validate_ddl(
        {"columns": ["dupe_col", "dupe_col"], "table_properties": {}, "post_hook": []}
    )
    assert result.is_err


def test_ddl_cols08():
    result = validate_ddl(
        {
            "columns": ["dupe_col", {"name": "dupe_col"}],
            "table_properties": {},
            "post_hook": [],
        }
    )
    assert result.is_err


# def test_ddl_idx01():
#     result = validate_ddl({"indexes": {"idx1": {"columns": ["col1", "col2"]}}})
#     assert result.is_ok and result.value == {
#         "columns": [],
#         "indexes": {"idx1": {"columns": ["col1", "col2"]}},
#         "primary_key": [],
#         "permissions": {},
#     }
#
#
# def test_ddl_idx02():
#     result = validate_ddl(
#         {
#             "indexes": {
#                 "idx1": {"columns": ["col1", "col2"]},
#                 "idx2": {"columns": ["col1", "col2"]},
#             }
#         }
#     )
#     assert result.is_ok and result.value == {
#         "columns": [],
#         "indexes": {
#             "idx1": {"columns": ["col1", "col2"]},
#             "idx2": {"columns": ["col1", "col2"]},
#         },
#         "primary_key": [],
#         "permissions": {},
#     }
#
#
# def test_ddl_pk01():
#     result = validate_ddl(
#         {
#             "columns": ["col1", "col2"],
#             "indexes": {"primary_key": {"columns": ["col1", "col2"]}},
#         }
#     )
#     assert result.is_ok and result.value == {
#         "columns": [
#             {
#                 "name": "col1",
#                 "type": None,
#                 "primary": False,
#                 "not_null": False,
#                 "unique": False,
#                 "dst_name": None,
#             },
#             {
#                 "name": "col2",
#                 "type": None,
#                 "primary": False,
#                 "not_null": False,
#                 "unique": False,
#                 "dst_name": None,
#             },
#         ],
#         "indexes": {},
#         "primary_key": ["col1", "col2"],
#         "permissions": {},
#     }
#
#
# def test_ddl_pk02():
#     result = validate_ddl(
#         {
#             "columns": [
#                 {"name": "col1", "primary": True},
#                 {"name": "col2", "primary": True},
#             ],
#         }
#     )
#     assert result.is_ok and result.value == {
#         "columns": [
#             {
#                 "name": "col1",
#                 "type": None,
#                 "primary": True,
#                 "not_null": False,
#                 "unique": False,
#                 "dst_name": None,
#             },
#             {
#                 "name": "col2",
#                 "type": None,
#                 "primary": True,
#                 "not_null": False,
#                 "unique": False,
#                 "dst_name": None,
#             },
#         ],
#         "indexes": {},
#         "primary_key": ["col1", "col2"],
#         "permissions": {},
#     }
#
#
# def test_ddl_pk03():
#     result = validate_ddl(
#         {"indexes": {"primary_key": {"columns": ["col1", "col2"]}}})
#     assert result.is_ok and result.value == {
#         "columns": [],
#         "indexes": {},
#         "primary_key": ["col1", "col2"],
#         "permissions": {},
#     }
#
#
# def test_ddl_pk04():
#     result = validate_ddl({"indexes": {"primary_key": {"columns": []}}})
#     assert result.is_err
#
#
# def test_ddl_pk05():
#     result = validate_ddl(
#         {"columns": ["col1"], "indexes": {
#             "primary_key": {"columns": ["col1", "col2"]}}}
#     )
#     assert result.is_err
