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


def get_query_component(query):
    match = RE_TASK_QUERY.match(query)
    if match is None:
        raise DagQueryError(f'Incorrect task query syntax "{query}"')
    else:
        match_components = match.groupdict()
        print(match_components)
        if match_components.get("tag") is not None:
            return {"type": "tag", "value": match_components["tag"]}
        if match_components.get("dag") is not None:
            return {"type": "dag", "value": match_components["dag"]}
        if match_components.get("task"):
            return {
                "type": "task",
                "value": match_components["task"],
                "upstream": match_components.get("upstream", "") == "+",
                "downstream": match_components.get("downstream", "") == "+",
            }
    raise DagQueryError("Unknown error")


def get_query(include=list(), exclude=list()):
    overlap = set(include).intersection(set(exclude))
    if len(overlap) > 0:
        overlap = ", ".join(overlap)
        raise DagQueryError(f'Overlap between include and exclude for "{overlap}"')

    include = [get_query_component(q) for q in include]
    for comp in include:
        comp["operation"] = "include"

    exclude = [get_query_component(q) for q in exclude]
    for comp in exclude:
        comp["operation"] = "exclude"

    return include + exclude
