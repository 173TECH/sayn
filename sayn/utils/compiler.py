from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from jinja2 import Environment, FileSystemLoader, StrictUndefined, Template

from ..core.errors import SaynCompileError, SaynMissingFileError


class BaseCompiler(ABC):
    @abstractmethod
    def compile(self, obj: Union[Template, Path, str], **kwargs) -> str:
        pass

    @abstractmethod
    def compile_prod(self, obj: Union[Path, str], **kwargs) -> str:
        pass


class Compiler(BaseCompiler):
    def __init__(self, parameters, prod_parameters):
        self.base_env = Environment(
            loader=FileSystemLoader(Path(".")),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

        self.env = self.base_env.overlay()
        self.env.globals.update(**parameters)

        self.prod_env = self.base_env.overlay()
        self.prod_env.globals.update(**prod_parameters)

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
                return env.get_template(str(obj))

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

    def compile(self, obj: Union[Template, Path, str], **kwargs) -> str:
        template = self._get_template(obj, False)
        return self._compile_template(template, **kwargs)

    def compile_prod(self, obj: Union[Path, str], **kwargs) -> str:
        template = self._get_template(obj, False)
        return self._compile_template(template, **kwargs)

    # Factories
    def get_task_compiler(self, task) -> BaseCompiler:
        return TaskCompiler(self.env, self.prod_env, task)

    # def get_db_object_compiler(self) -> BaseCompiler:
    #     pass


class TaskCompiler(Compiler):
    def __init__(self, base_env, base_prod_env, task) -> None:
        self.env = base_env.overlay()
        self.env.globals["task"] = task

        self.prod_env = base_prod_env.overlay()
        self.prod_env.globals["task"] = task
