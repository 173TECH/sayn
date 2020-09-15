import pytest

from sayn.utils.task_query import get_query


tasks = {
    "task1": {"dag": "dag1", "tags": list()},
    "task2": {"dag": "dag1", "tags": ["tag1"]},
    "task3": {"dag": "dag2", "tags": ["tag1"]},
    "task4": {"dag": "dag2", "tags": list()},
    "task5": {"dag": "dag3", "tags": ["tag1", "tag2"]},
    "task6": {"dag": "dag3", "tags": list()},
    "task7": {"dag": "dag3", "tags": list()},
}


def test_simple01():
    assert get_query(tasks, include=["task1"]).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        }
    ]


def test_simple02():
    assert get_query(tasks, include=["task_undefined"]).is_err


def test_simple03():
    assert get_query(tasks, include=["task2+"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": True,
        }
    ]


def test_simple04():
    assert get_query(tasks, include=["+task4+"]).value == [
        {"operation": "include", "task": "task4", "upstream": True, "downstream": True}
    ]


def test_dag01():
    assert get_query(tasks, include=["dag:dag1"]).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_tag01():
    assert get_query(tasks, include=["tag:tag1"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_error_identifier01():
    assert get_query(tasks, include=["+_task_undefined"]).is_err


def test_error_identifier02():
    assert get_query(tasks, include=["tag:tag_undefined"]).is_err


def test_error_identifier03():
    assert get_query(tasks, include=["dag:dag_undefined"]).is_err


def test_full_query02():
    assert get_query(tasks, include=["tag:tag1"], exclude=["dag:dag2"]).value == [
        {
            "operation": "include",
            "task": "task2",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task4",
            "upstream": False,
            "downstream": False,
        },
    ]


def test_full_query03():
    assert get_query(
        tasks,
        include=["task1", "task2+", "tag:tag1", "+task2"],
        exclude=["dag:dag3", "+task3"],
    ).value == [
        {
            "operation": "include",
            "task": "task1",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task2",
            "upstream": True,
            "downstream": True,
        },
        {
            "operation": "include",
            "task": "task3",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "include",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task5",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task6",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task7",
            "upstream": False,
            "downstream": False,
        },
        {
            "operation": "exclude",
            "task": "task3",
            "upstream": True,
            "downstream": False,
        },
    ]
