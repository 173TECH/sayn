import pytest

from sayn.app.common import get_query, get_query_component, DagQueryError


def test_simple01():
    assert get_query_component("task_name") == {
        "type": "task",
        "value": "task_name",
        "upstream": False,
        "downstream": False,
    }


def test_simple02():
    assert get_query_component("+task_name") == {
        "type": "task",
        "value": "task_name",
        "upstream": True,
        "downstream": False,
    }


def test_simple03():
    assert get_query_component("task_name+") == {
        "type": "task",
        "value": "task_name",
        "upstream": False,
        "downstream": True,
    }


def test_simple04():
    assert get_query_component("+task_name+") == {
        "type": "task",
        "value": "task_name",
        "upstream": True,
        "downstream": True,
    }


def test_dag01():
    assert get_query_component("dag:dag_name") == {
        "type": "dag",
        "value": "dag_name",
    }


def test_tag01():
    assert get_query_component("tag:tag_name") == {
        "type": "tag",
        "value": "tag_name",
    }


def test_error_identifier01():
    with pytest.raises(DagQueryError):
        get_query_component("+_task_name")


def test_full_query01():
    assert get_query(include=["tag:tag_name"]) == [
        {"operation": "include", "type": "tag", "value": "tag_name"}
    ]


def test_full_query02():
    assert get_query(include=["tag:tag_name"], exclude=["dag:dag_name"]) == [
        {"operation": "include", "type": "tag", "value": "tag_name"},
        {"operation": "exclude", "type": "dag", "value": "dag_name"},
    ]


def test_full_query03():
    assert get_query(
        include=["task_name1", "task_name2"], exclude=["dag:dag_name", "task_name3+"]
    ) == [
        {
            "operation": "include",
            "type": "task",
            "value": "task_name1",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "type": "task",
            "value": "task_name2",
            "upstream": False,
            "downstream": False,
        },
        {"operation": "exclude", "type": "dag", "value": "dag_name"},
        {
            "operation": "exclude",
            "type": "task",
            "value": "task_name3",
            "upstream": False,
            "downstream": True,
        },
    ]


def test_error_full_query01():
    with pytest.raises(DagQueryError):
        get_query(include=["tag:tag_name"], exclude=["tag:tag_name"])
