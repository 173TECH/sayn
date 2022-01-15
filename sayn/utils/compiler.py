from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Union

from jinja2 import Environment, FileSystemLoader, StrictUndefined, Template

from ..core.errors import SaynCompileError, SaynMissingFileError


class TaskJinjaEnv:
    """This class is the `task` object when compiling task configurations"""

    def __init__(self, name=None, group=None):
        if group is not None:
            self.group = group
        if name is not None:
            self.name = name


class BaseCompiler(ABC):
    @abstractmethod
    def compile(self, obj: Union[Template, Path, str], **kwargs) -> str:
        pass

    @abstractmethod
    def compile_prod(self, obj: Union[Path, str], **kwargs) -> str:
        pass


class Compiler(BaseCompiler):
    def __init__(self, run_arguments, parameters, prod_parameters):
        env_arguments = {
            "full_load": run_arguments.full_load,
            "start_dt": f"'{run_arguments.start_dt.strftime('%Y-%m-%d')}'",
            "end_dt": f"'{run_arguments.end_dt.strftime('%Y-%m-%d')}'",
        }

        self.env = self._create_environment()
        self.env.globals.update(**env_arguments)
        self.env.globals.update(**parameters)

        self.prod_env = self._create_environment()
        self.prod_env.globals.update(**env_arguments)
        self.prod_env.globals.update(**prod_parameters)

    def _create_environment(self):
        return Environment(
            loader=FileSystemLoader(Path(".")),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

    def _get_template(
        self, obj: Union[Template, Path, str], use_prod: bool = False
    ) -> Template:
        if use_prod:
            env = self.prod_env
        else:
            env = self.env

        if isinstance(obj, Template):
            return obj
        elif isinstance(obj, Path):
            if not obj.is_file():
                raise SaynMissingFileError(str(obj))
            else:
                return env.from_string(obj.read_text())

        elif isinstance(obj, str):
            return env.from_string(obj)

        else:
            raise SaynCompileError(f'Cannot compile object of type "{type(obj)}"')

    def _compile_template(self, template: Template, **kwargs) -> str:
        out = template.render(**kwargs)
        if out is None:
            return ""
        else:
            return out

    def add_global(self, name, obj):
        self.env.globals[name] = obj
        self.prod_env.globals[name] = obj

    def update_globals(self, **params):
        self.env.globals.update(**params)
        self.prod_env.globals.update(**params)

    def compile(self, obj: Union[Template, Path, str], **kwargs) -> str:
        template = self._get_template(obj, False)
        return self._compile_template(template, **kwargs)

    def compile_prod(self, obj: Union[Path, str], **kwargs) -> str:
        template = self._get_template(obj, False)
        return self._compile_template(template, **kwargs)

    def prepare(self, obj):
        return Prepared(self, self._get_template(obj))

    # Factories
    def get_task_compiler(self, group, name) -> BaseCompiler:
        return TaskCompiler(
            self.env, self.prod_env, TaskJinjaEnv(group=group, name=name)
        )

    # def get_db_object_compiler(self) -> BaseCompiler:
    #     pass


class Prepared:
    """A prepared statement ready to be compiled"""

    def __init__(self, compiler, template):
        self.compiler = compiler
        self.template = template

    def compile(self, **kwargs):
        return self.compiler.compile(self.template, **kwargs)

    def compile_prod(self, **kwargs):
        return self.compiler.compile_prod(self.template, **kwargs)


class TaskCompiler(Compiler):
    def __init__(self, base_env, base_prod_env, task) -> None:
        self.env = self._create_environment()
        self.env.globals.update(**deepcopy(base_env.globals))
        self.env.globals["task"] = task

        self.prod_env = self._create_environment()
        self.prod_env.globals.update(**deepcopy(base_prod_env.globals))
        self.prod_env.globals["task"] = task
