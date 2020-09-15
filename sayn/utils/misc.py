from datetime import datetime, timedelta
from copy import deepcopy
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


def merge_dicts(into_dict, from_dict):
    if isinstance(into_dict, list):
        if not isinstance(from_dict, list):
            return deepcopy(from_dict)
        else:
            return deepcopy(into_dict + from_dict)
    elif isinstance(into_dict, dict):
        if not isinstance(from_dict, dict):
            return deepcopy(from_dict)
        else:
            output = dict()
            for k in set(into_dict.keys()).union(from_dict.keys()):
                if k in from_dict and k in into_dict:
                    output[k] = merge_dicts(into_dict[k], from_dict[k])
                else:
                    output[k] = deepcopy(into_dict.get(k) or from_dict.get(k))

            return output
    else:
        return from_dict


def merge_dict_list(dict_list):
    into_dict = dict_list[0]
    for from_dict in dict_list[1:]:
        into_dict = merge_dicts(into_dict, from_dict)

    return into_dict


def map_nested(ob, func):
    if isinstance(ob, dict):
        return {k: map_nested(v, func) for k, v in ob.items()}
    elif isinstance(ob, list):
        return [map_nested(e, func) for e in ob]
    else:
        return func(ob)


def group_list(items):
    return {
        k: [vv[1] for vv in v]
        for k, v in groupby(sorted(items, key=lambda x: x[0]), lambda x: x[0])
    }
