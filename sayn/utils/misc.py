from itertools import groupby


def reverse_dict(dag):
    return {
        i[0]: [ii[1] for ii in i[1]]
        for i in groupby(
            sorted(
                [(parent, node) for node, parents in dag.items() for parent in parents]
            ),
            lambda x: x[0],
        )
    }


def reverse_dict_inclusive(dag):
    rev = {
        i[0]: [ii[1] for ii in i[1]]
        for i in groupby(
            sorted(
                [(parent, node) for node, parents in dag.items() for parent in parents]
            ),
            lambda x: x[0],
        )
    }
    rev.update({n: list() for n in dag.keys() if n not in rev})
    return rev
