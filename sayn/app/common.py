import re

RE_TASK_QUERY = re.compile(
    (
        r"^("
        r"(?!(dag:|tag:))(?P<upstream>\+?)(?P<task>[a-zA-Z0-9][-_a-zA-Z0-9]+)(?P<downstream>\+?)|"
        r"dag:(?P<dag>[a-zA-Z0-9][-_a-zA-Z0-9]+)|"
        r"tag:(?P<tag>[a-zA-Z0-9][-_a-zA-Z0-9]+)"
        r")$"
    )
)


class DagQueryError(Exception):
    pass


def _get_query_component(tasks, query):
    match = RE_TASK_QUERY.match(query)
    if match is None:
        raise DagQueryError(f'Incorrect task query syntax "{query}"')
    else:
        match_components = match.groupdict()

        if match_components.get("tag") is not None:
            tag = match_components["tag"]
            relevant_tasks = {k: v for k, v in tasks.items() if tag in v.get("tags")}
            if len(relevant_tasks) == 0:
                raise DagQueryError(f'Undefined tag "{tag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("dag") is not None:
            dag = match_components["dag"]
            relevant_tasks = {k: v for k, v in tasks.items() if dag == v.get("dag")}
            if len(relevant_tasks) == 0:
                raise DagQueryError(f'Undefined dag "{dag}"')
            return [
                {"task": task, "upstream": False, "downstream": False}
                for task, value in relevant_tasks.items()
            ]

        if match_components.get("task") is not None:
            task = match_components["task"]
            if task not in tasks:
                raise DagQueryError(f'Undefined task "{task}"')
            return [
                {
                    "task": task,
                    "upstream": match_components.get("upstream", "") == "+",
                    "downstream": match_components.get("downstream", "") == "+",
                }
            ]


def get_query(tasks, include=list(), exclude=list()):
    overlap = set(include).intersection(set(exclude))
    if len(overlap) > 0:
        overlap = ", ".join(overlap)
        raise DagQueryError(f'Overlap between include and exclude for "{overlap}"')

    output = [
        dict(comp, operation=operation)
        for operation, components in (("include", include), ("exclude", exclude))
        for q in components
        for comp in _get_query_component(tasks, q)
    ]

    # simplify the queries by unifying upstream/downstream
    include = dict()
    exclude = dict()
    for operand in output:
        operation_dict = include if operand["operation"] == "include" else exclude
        task = operand["task"]
        if task in operation_dict:
            for flag in ("upstream", "downstream"):
                operation_dict[task][flag] = operation_dict[task][flag] or operand[flag]
        else:
            operation_dict[task] = {
                "upstream": operand["upstream"],
                "downstream": operand["downstream"],
            }

    return [
        dict(flags, task=task, operation=operation)
        for operation, operands in (("include", include), ("exclude", exclude))
        for task, flags in operands.items()
    ]
