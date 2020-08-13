from .misc import reverse_dict, reverse_dict_inclusive

from ..core.errors import DagCycleError, DagMissingParentsError


def _has_missing_parents(dag):
    rev = reverse_dict(dag)
    missing = {p: c for p, c in rev.items() if p not in dag}
    if len(missing) > 0:
        raise DagMissingParentsError(missing)
    return False


def _is_cyclic_helper(dag, node, visited, stack, breadcrumbs):
    visited[node] = True
    stack[node] = True

    for parent in dag[node]:
        if parent == node:
            raise DagCycleError([node, parent])
        elif not visited[parent]:
            res = _is_cyclic_helper(dag, parent, visited, stack, breadcrumbs + [parent])
            if res is not None:
                raise DagCycleError(res)
        elif stack[parent]:
            raise DagCycleError(breadcrumbs + [parent])

    stack[node] = False


def _is_cyclic(dag):
    visited = {n: False for n in dag.keys()}
    stack = {n: False for n in dag.keys()}
    for node, parents in dag.items():
        if not visited[node]:
            res = _is_cyclic_helper(dag, node, visited, stack, [node])
            if res is not None:
                return res

    return True


def dag_is_valid(dag):
    _has_missing_parents(dag)
    _is_cyclic(dag)

    return True


# DAG -> Sorted list
def topological_sort(dag):
    if len(dag) == 0:
        return list()
    dag_is_valid(dag)
    topo_sorted = []
    pending = list(dag.keys())
    current = 0
    while True:
        if len(set(dag[pending[current]]) - set(topo_sorted)) == 0:
            topo_sorted.append(pending.pop(current))

            if len(pending) == 0:
                return topo_sorted
        else:
            current += 1

        if current == len(pending):
            current = 0


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

    return to_include


def query(dag, query=list()):
    topo_sort = topological_sort(dag)
    if len(query) == 0:
        return topo_sort

    query = sorted(query, key=lambda x: 0 if x["operation"] == "include" else 1)
    to_include = set()
    query_cache = dict()

    for operand in query:
        key = (operand["task"], operand["upstream"], operand["downstream"])
        if key in query_cache:
            tasks = query_cache[key]
        else:
            tasks = [key[0]]
            if key[1]:
                tasks.extend(upstream(dag, key[0]))
            if key[2]:
                tasks.extend(downstream(dag, key[0]))
            query_cache[key] = tasks

        if operand["operation"] == "include":
            to_include = to_include.union(tasks)
        else:
            to_include -= tasks

    return [n for n in topo_sort if n in to_include]
