from .misc import reverse_dict, reverse_dict_inclusive

from ..core.errors import Err, Ok


def _has_missing_parents(dag):
    missing = {}
    for task, parents in dag.items():
        missing_parents = [p for p in parents if p not in dag]
        if len(missing_parents) > 0:
            missing[task] = missing_parents
    if len(missing) > 0:
        return Err("dag", "missing_parents", missing=missing)
    return Ok(False)


def _is_cyclic_helper(dag, node, visited, stack, breadcrumbs):
    visited[node] = True
    stack[node] = True

    for parent in dag[node]:
        if parent == node:
            return Err("dag", "cycle_error", path=[node, parent])
        elif not visited[parent]:
            result = _is_cyclic_helper(
                dag, parent, visited, stack, breadcrumbs + [parent]
            )
            if result.is_err:
                return result
        elif stack[parent]:
            return Err("dag", "cycle_error", path=breadcrumbs + [parent])

    stack[node] = False

    return Ok()


def _is_cyclic(dag):
    visited = {n: False for n in dag.keys()}
    stack = {n: False for n in dag.keys()}
    for node, parents in dag.items():
        if not visited[node]:
            result = _is_cyclic_helper(dag, node, visited, stack, [node])
            if result.is_err:
                return result

    return Ok(True)


def dag_is_valid(dag):
    result = _has_missing_parents(dag)
    if result.is_err:
        return result
    result = _is_cyclic(dag)
    if result.is_err:
        return result

    return Ok()


# DAG -> Sorted list
def topological_sort(dag):
    if len(dag) == 0:
        return Ok(list())
    result = dag_is_valid(dag)
    if result.is_err:
        return result
    topo_sorted = []
    pending = list(dag.keys())
    current = 0
    while True:
        if len(set(dag[pending[current]]) - set(topo_sorted)) == 0:
            topo_sorted.append(pending.pop(current))

            if len(pending) == 0:
                return Ok(topo_sorted)
        else:
            current += 1

        if current == len(pending):
            current = 0

    return Ok(topo_sorted)


# DAG querying
def downstream(dag, node):
    return upstream(reverse_dict_inclusive(dag), node)


def upstream(dag, node):
    to_include = list()
    queue = dag[node]
    while len(queue) > 0:
        current = queue.pop(0)
        if current not in to_include:
            to_include.append(current)
            queue.extend(dag[current])

    return Ok(to_include)


def query(dag, query=list()):
    result = topological_sort(dag)
    if result.is_err:
        return result
    elif len(query) == 0:
        return Ok(result.value)
    else:
        topo_sort = result.value

    query = sorted(query, key=lambda x: 0 if x["operation"] == "include" else 1)
    if query[0]["operation"] == "include":
        to_include = set()
    else:
        to_include = set(topo_sort)
    query_cache = dict()

    for operand in query:
        key = (operand["task"], operand["upstream"], operand["downstream"])
        if key in query_cache:
            tasks = query_cache[key]
        else:
            tasks = [key[0]]
            if key[1]:
                result = upstream(dag, key[0])
                if result.is_err:
                    return result
                tasks.extend(result.value)
            if key[2]:
                result = downstream(dag, key[0])
                if result.is_err:
                    return result
                tasks.extend(result.value)
            query_cache[key] = tasks

        if operand["operation"] == "include":
            to_include = to_include.union(tasks)
        else:
            to_include -= set(tasks)

    return Ok([n for n in topo_sort if n in to_include])
