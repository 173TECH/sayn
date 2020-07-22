from .misc import reverse_dict, reverse_dict_inclusive


class MissingParentsError(Exception):
    def __init__(self, missing):
        message = "Some referenced tasks are missing: " + "; ".join(
            [
                f'"{parent}" referenced by ({", ".join(children)})'
                for parent, children in missing.items()
            ]
        )
        super(MissingParentsError, self).__init__(message)

        self.missing = missing


class CycleError(Exception):
    def __init__(self, cycle):
        message = f"Cycle found in the DAG {' -> '.join(cycle)}"
        super(CycleError, self).__init__(message)

        self.cycle = cycle


def _has_missing_parents(dag):
    rev = reverse_dict(dag)
    missing = {p: c for p, c in rev.items() if p not in dag}
    if len(missing) > 0:
        raise MissingParentsError(missing)
    return False


def _is_cyclic_helper(dag, node, visited, stack, breadcrumbs):
    visited[node] = True
    stack[node] = True

    for parent in dag[node]:
        if parent == node:
            raise CycleError([node, parent])
        elif not visited[parent]:
            res = _is_cyclic_helper(dag, parent, visited, stack, breadcrumbs + [parent])
            if res is not None:
                raise CycleError(res)
        elif stack[parent]:
            raise CycleError(breadcrumbs + [parent])

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


def is_valid(dag):
    _has_missing_parents(dag)
    _is_cyclic(dag)

    return True


# DAG -> Sorted list
def topological_sort(dag):
    is_valid(dag)
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
def query(dag, query):
    topo_sort = topological_sort(dag)
    include_upstream = False
    include_downstream = False
    if query[0] == "+":
        include_upstream = True
        query = query[1:]
    if query[-1] == "+":
        include_downstream = True
        query = query[:-1]

    to_include = [query]
    if include_upstream:
        to_include.extend(upstream(dag, query))
    if include_downstream:
        to_include.extend(downstream(dag, query))

    return [n for n in topo_sort if n in to_include]


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
