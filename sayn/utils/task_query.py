import re

from ..core.errors import Err, Ok

#####################################
# Task query interpretation functions
#####################################

RE_TASK_QUERY = re.compile(
    (
        r"^("
        r"(?!(dag:|tag:))(?P<upstream>\+?)(?P<task>[a-zA-Z0-9][-_a-zA-Z0-9]+)(?P<downstream>\+?)|"
        r"dag:(?P<dag>[a-zA-Z0-9][-_a-zA-Z0-9]+)|"
        r"tag:(?P<tag>[a-zA-Z0-9][-_a-zA-Z0-9]+)"
        r")$"
    )
)


def _get_query_component(tasks, query):
    tasks = {
        k: {"dag": v["dag"], "tags": v.get("tags", list())} for k, v in tasks.items()
    }
    match = RE_TASK_QUERY.match(query)
    if match is None:
        return Err("task_query", "incorrect_syntax", query=query,)
    else:
        match_components = match.groupdict()

        if match_components.get("tag") is not None:
            tag = match_components["tag"]
            relevant_tasks = {k: v for k, v in tasks.items() if tag in v.get("tags")}
            if len(relevant_tasks) == 0:
                return Err("task_query", "undefined_tag", tag=tag,)
            return Ok(
                [
                    {"task": task, "upstream": False, "downstream": False}
                    for task, value in relevant_tasks.items()
                ]
            )

        if match_components.get("dag") is not None:
            dag = match_components["dag"]
            relevant_tasks = {k: v for k, v in tasks.items() if dag == v.get("dag")}
            if len(relevant_tasks) == 0:
                return Err("task_query", "undefined_dag", dag=dag,)
            return Ok(
                [
                    {"task": task, "upstream": False, "downstream": False}
                    for task, value in relevant_tasks.items()
                ]
            )

        if match_components.get("task") is not None:
            task = match_components["task"]
            if task not in tasks:
                return Err("task_query", "undefined_task", task=task,)
            return Ok(
                [
                    {
                        "task": task,
                        "upstream": match_components.get("upstream", "") == "+",
                        "downstream": match_components.get("downstream", "") == "+",
                    }
                ]
            )


def get_query(tasks, include=list(), exclude=list()):
    overlap = set(include).intersection(set(exclude))
    if len(overlap) > 0:
        overlap = ", ".join(overlap)
        return Err("task_query", "query_overlap", overlap=overlap,)

    output = list()
    for operation, components in (("include", include), ("exclude", exclude)):
        for q in components:
            result = _get_query_component(tasks, q)
            if result.is_err:
                return result
            else:
                components = result.value
            for comp in components:
                output.append(dict(comp, operation=operation))

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

    return Ok(
        [
            dict(flags, task=task, operation=operation)
            for operation, operands in (("include", include), ("exclude", exclude))
            for task, flags in operands.items()
        ]
    )
