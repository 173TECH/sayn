from copy import deepcopy
from pathlib import Path
from typing import Any, List, Mapping, Optional

from pydantic import BaseModel, Field, validator, Extra
from sayn.tasks.python import DecoratorTaskWrapper

from ..utils.compiler import TaskJinjaEnv
from ..utils.misc import merge_dicts, merge_dict_list
from ..utils.dag import upstream, topological_sort
from ..utils.yaml import read_yaml_file
from .errors import Err, Ok


class Project(BaseModel):
    required_credentials: List[str]
    default_db: Optional[str]
    parameters: Optional[Mapping[str, Any]]
    presets: Optional[Mapping[str, Mapping[str, Any]]]
    autogroups: Mapping[str, Any] = Field(dict(), alias="groups")

    # database_prefix: Optional[str]
    # database_suffix: Optional[str]
    # database_override: Optional[str]
    schema_prefix: Optional[str]
    schema_suffix: Optional[str]
    schema_override: Optional[str]
    table_prefix: Optional[str]
    table_suffix: Optional[str]
    table_override: Optional[str]

    class Config:
        extra = Extra.forbid
        anystr_lower = True

    @validator("required_credentials")
    def required_credentials_are_unique(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("Duplicate values found in required_credentials")
        return v

    @validator("default_db", always=True)
    def default_db_exists(cls, v, values):
        if "required_credentials" not in values:
            return v

        if v is None and len(values["required_credentials"]) == 1:
            return values["required_credentials"][0]
        elif v is None:
            raise ValueError("Missing default_db in project.yaml")
        elif v not in values["required_credentials"]:
            raise ValueError(f'default_db value "{v}" not in required_credentials')

        return v

    @validator(
        *[
            f"{o}_{t}"
            for o in ("table", "schema")
            for t in ("prefix", "suffix", "override")
        ],
        allow_reuse=True,
    )
    def non_empty_strings(cls, v):
        if len(v.strip()) != 0:
            return v.strip()


def read_project(project_root=Path(".")):
    return read_yaml_file(project_root / Path("project.yaml"), Project)


class TaskGroupFile(BaseModel):
    presets: Optional[Mapping[str, Mapping[str, Any]]]
    tasks: Optional[Mapping[str, Mapping[str, Any]]]
    tests: Optional[Mapping[str, Mapping[str, Any]]]

    class Config:
        extra = Extra.forbid
        anystr_lower = True


def read_groups(project_root=Path(".")):
    task_folder = project_root / "tasks"
    if task_folder.exists() and task_folder.is_dir():
        out = dict()
        for file in task_folder.glob("*.yaml"):
            name = str(file.relative_to(task_folder))[:-5]
            out[name] = read_yaml_file(file, TaskGroupFile)

        return out
    else:
        return dict()


###############################
# Task related config functions
###############################


def get_presets(global_presets, groups):
    """Returns a dictionary of presets merged with the referenced preset
    Presets define a direct acyclic graph by including the `preset` property, so
    this function validates that there are no cycles and that all referenced presets
    are defined.
    In the output, preset names are prefixed with `sayn_global:` or `group:` so that we can
    merge all presets in the project in the same dictionary.
    Args:
      global_presets (dict): dictionary containing the presets defined in project.yaml
      groups (sayn.app.config.TaskGroup): a list of task groups from the tasks/ folder
    """
    # 1. Construct a dictionary of presets so we can attach that info to the tasks
    presets_info = {
        f"sayn_global:{k}": {kk: vv for kk, vv in v.items() if kk != "preset"}
        for k, v in global_presets.items()
    }

    # 1.1. We start with the global presets defined in project.yaml
    presets_project = {
        k: [f"sayn_global:{v}"] if v is not None else []
        for k, v in {
            f"sayn_global:{name}": preset.get("preset")
            for name, preset in global_presets.items()
        }.items()
    }

    # 1.2. Then we add the presets defined in the task groups
    for group_name, group in groups.items():
        if group.presets is not None:
            presets_info.update(
                {
                    f"{group_name}:{k}": {
                        kk: vv for kk, vv in v.items() if kk != "preset"
                    }
                    for k, v in group.presets.items()
                }
            )

        if group.presets is None:
            group_presets = dict()
        else:
            group_presets = {
                name: preset.get("preset") for name, preset in group.presets.items()
            }

        # Check if the preset referenced is defined in the task group, otherwise, point at the
        # global task group
        group_presets = {
            f"{group_name}:{k}": [
                f"{group_name}:{v}"
                if v in group_presets and v != k
                else f"sayn_global:{v}"
            ]
            if v is not None
            else []
            for k, v in group_presets.items()
        }
        presets_project.update(group_presets)

    # 1.3. The preset references represent a dag that we need to validate, ensuring
    #      there are no cycles and that all references exists
    result = topological_sort(presets_project)
    if result.is_err:
        return result
    else:
        topo_sort = result.value

    # 1.4. Merge the presets with the reference preset, so that we have 1 dictionary
    #      per preset a task could reference
    presets = {
        name: merge_dict_list(
            [presets_info[p] for p in upstream(presets_project, name).value]
            + [presets_info[name]]
        )
        for name in topo_sort
    }

    return Ok(presets)


def get_task_dict(task, task_name, group_name, presets):
    """Returns a single task merged with the referenced preset
    Args:
      task (dict): a dictionary with the task information
      task_name (str): the name of the task
      group_name (str): the name of the group it appeared on
      presets (dict): a dictionary of merged presets returned by get_presets
    """
    if "preset" in task:
        preset_name = task["preset"]
        preset = presets.get(
            f"{group_name}:{preset_name}",
            presets.get(f"sayn_global:{preset_name}"),
        )
        if preset is None:
            return Err(
                "get_task_dict",
                "missing_preset",
                group=group_name,
                task=task_name,
                preset=preset_name,
            )
        task = merge_dicts(preset, task)

    return Ok(dict(task, name=task_name, group=group_name))


def get_tasks_dict(
    global_presets, groups, autogroups, sql_folder, compiler, python_loader
):
    """Returns a dictionary with the task definition with the preset information merged
    Args:
      global_presets (dict): a dictionary with the presets as defined in project.yaml
      groups (sayn.common.config.TaskGroup): a list of task groups from the tasks/ folder
    """
    result = get_presets(global_presets, groups)
    if result.is_err:
        return result
    else:
        presets = result.value

    errors = dict()
    tasks = dict()
    tests = dict()

    for group_name, group in groups.items():
        if group.tasks is not None:
            for task_name, task in group.tasks.items():
                if task_name in tasks:
                    return Err(
                        "dag",
                        "duplicate_task",
                        task=task_name,
                        groups=(group_name, tasks[task_name]["group"]),
                    )
                result = get_task_dict(task, task_name, group_name, presets)
                if result.is_ok and "type" in result.value:
                    tasks[task_name] = result.value
                    if tasks[task_name].get("type") == "test":
                        return Err(
                            "dag",
                            "test in tasks",
                            task=task_name,
                        )
                else:
                    if "type" in result.value:
                        errors[task_name] = result.error
                    else:
                        errors[task_name] = Err(
                            "get_task_dict",
                            "no_type",
                            group=group_name,
                            task=task_name,
                        )

        if group.tests is not None:
            for test_name, test in group.tests.items():
                if "type" in test.keys():
                    return Err(
                        "dag",
                        "Tests have no types",
                        task=test_name,
                    )

                test["type"] = "test"

                if test_name in tests:
                    return Err(
                        "dag",
                        "duplicate_task",
                        task=test_name,
                        groups=(group_name, tasks[test_name]["group"]),
                    )

                result = get_task_dict(test, test_name, group_name, presets)
                if result.is_ok:
                    tests[test_name] = result.value
                else:
                    errors[test_name] = result.error

    for group_name, group in autogroups.items():
        group_definition = {"group": group_name}
        if "preset" in group:
            preset_name = f"sayn_global:{group['preset']}"
            if preset_name not in presets:
                return Err("dag", "missing_preset", preset_name=preset_name)
            group_definition.update(deepcopy(presets[preset_name]))

        group_definition.update(group)

        if group_definition.get("type") in ("sql", "autosql"):
            if "file_name" not in group_definition:
                return Err(
                    "dag",
                    "missing_file_name",
                    group=group_name,
                    type=group_definition.get("type"),
                )

            file_glob = compiler.compile(
                group_definition["file_name"], task=TaskJinjaEnv(group=group_name)
            )
            found_file = False
            for file in Path(sql_folder).glob(file_glob):
                found_file = True
                task_name = file.stem
                if task_name in tasks:
                    return Err(
                        "dag",
                        "duplicate_task",
                        task_name=task_name,
                        groups=(group_name, tasks[task_name]["group"]),
                    )

                task = deepcopy(group_definition)
                task["file_name"] = str(file.relative_to(sql_folder))

                result = get_task_dict(task, task_name, group_name, presets)
                if result.is_ok:
                    tasks[task_name] = result.value
                else:
                    errors[task_name] = result.error

                tasks[task_name] = task

            if not found_file:
                return Err("dag", "empty_group", group=group_name)

        elif group_definition.get("type") == "python":
            group_definition["group"] = group_name
            group_definition["type"] = "python_module"
            if "module" not in group_definition:
                return Err("task", "missing_module", group=group_name)
            module_path = group_definition.pop("module", None)
            result = python_loader.get_objects(
                "python_tasks", module_path, DecoratorTaskWrapper
            )
            if result.is_err:
                return result
            else:
                objects = result.value

            for task_obj in objects:
                task_name = task_obj.func.__name__
                task = deepcopy(group_definition)
                task["name"] = task_name
                task["task_class"] = task_obj

                result = get_task_dict(task, task_name, group_name, presets)
                if result.is_ok:
                    tasks[task_name] = result.value
                else:
                    errors[task_name] = result.error

        # elif group_definition.get("type") == "python":
        #     group_definition["group"] = group_name
        #     group_tasks = group_definition.pop("tasks", dict())
        #     if not isinstance(group_tasks, dict):
        #         return Err(
        #             "dag",
        #             "wrong_yaml_type",
        #             group=group_name,
        #             type=type(group_definition.get("tasks")),
        #         )

        #     for task_name, task_def in group_tasks.items():
        #         if task_name in tasks:
        #             return Err(
        #                 "dag",
        #                 "duplicate_task",
        #                 task_name=task_name,
        #                 groups=(group_name, tasks[task_name]["group"]),
        #             )

        #         if task_def is not None:
        #             if not isinstance(task_def, dict):
        #                 return Err(
        #                     "dag",
        #                     "wrong_yaml_type",
        #                     group=group_name,
        #                     type=type(task_def),
        #                 )

        #             task = deepcopy(group_definition)
        #             task = merge_dicts(task, task_def)
        #         else:
        #             task = deepcopy(group_definition)

        #         result = get_task_dict(task, task_name, group_name, presets)
        #         if result.is_ok:
        #             tasks[task_name] = result.value
        #         else:
        #             errors[task_name] = result.error

        else:
            return Err(
                "dag",
                "wrong_autogroup_type",
                group=group_name,
                type=group_definition.get("type"),
            )

    for t in tests:
        if t in tasks.keys():
            return Err("dag", "Duplicate tests in tasks", task=t)

    if len(errors) > 0:
        return Err("get_tasks_dict", "task_parsing_error", errors=errors)
    else:
        return Ok(merge_dicts(tasks, tests))
