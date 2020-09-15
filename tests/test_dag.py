import pytest

from sayn.utils import dag


def test_valid01():
    test_dag = {"task1": ["task2", "task3"], "task2": [], "task3": []}
    assert dag.dag_is_valid(test_dag).is_ok


def test_valid02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.dag_is_valid(test_dag).is_ok


def test_missing_parents01():
    test_dag = {
        "task1": ["task2", "task3"],
        "task2": ["task3", "adf"],
        "task3": ["task4"],
    }
    assert dag.dag_is_valid(test_dag).is_err


def test_cycle01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task2"], "task3": []}
    assert dag.dag_is_valid(test_dag).is_err


def test_cycle02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task1"], "task3": []}
    assert dag.dag_is_valid(test_dag).is_err


def test_cycle03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": ["task1"]}
    assert dag.dag_is_valid(test_dag).is_err


def test_topological_sort01():
    test_dag = {"task1": [], "task2": [], "task3": []}
    assert dag.topological_sort(test_dag).value == ["task1", "task2", "task3"]


def test_topological_sort02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.topological_sort(test_dag).value == ["task3", "task2", "task1"]


def test_upstream01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert set(dag.upstream(test_dag, "task1").value) == set(["task3", "task2"])


def test_downstream02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.downstream(test_dag, "task1").value == []


def test_downstream03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.downstream(test_dag, "task2").value == ["task1"]


def test_downstream04():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert set(dag.downstream(test_dag, "task3").value) == set(["task2", "task1"])


def test_upstream05():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.upstream(test_dag, "task3").value == []


def test_query00():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(test_dag).value == ["task3", "task2", "task1"]


def test_query01():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": False,
                "upstream": False,
            }
        ],
    ).value == ["task3"]


def test_query02():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": True,
                "upstream": False,
            }
        ],
    ).value == ["task3", "task2", "task1"]


def test_query03():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": False,
                "upstream": True,
            }
        ],
    ).value == ["task3"]


def test_query04():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task3"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": True,
                "upstream": True,
            }
        ],
    ).value == ["task3", "task2", "task1"]


def test_cycle05():
    test_dag = {"task1": ["task2", "task3"], "task2": ["task2"], "task3": []}
    assert dag.query(
        test_dag,
        [
            {
                "operation": "include",
                "task": "task3",
                "downstream": True,
                "upstream": True,
            }
        ],
    ).is_err
