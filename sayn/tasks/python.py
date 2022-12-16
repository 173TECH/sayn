import inspect
from typing import Optional, List, Union
from enum import Enum


from ..database.unknown import UnknownDb
from .task import Task


class OnFailValue(str, Enum):
    skip = "skip"
    no_skip = "no_skip"


class PythonTask(Task):
    def config(self, **task_config):
        return self.success()

    def setup(self):
        return self.success()

    def run(self):
        return self.success()

    def compile(self):
        return self.success()

    def test(self):
        return self.success()


class DecoratorTask(PythonTask):
    def __init__(
        self,
        name,
        group,
        tracker,
        run_arguments,
        task_parameters,
        project_parameters,
        default_db,
        connections,
        compiler,
        src,
        out,
        sources,
        outputs,
        parents,
        tags,
        on_fail,
        func,
    ):
        super().__init__(
            name,
            group,
            tracker,
            run_arguments,
            task_parameters,
            project_parameters,
            default_db,
            connections,
            compiler,
            src,
            out,
        )

        self.compiler.update_globals(src=self.src, out=self.out)

        self._config_input["sources"].update(sources)
        self._config_input["outputs"].update(outputs)
        self._config_input["parents"].update(parents)
        self._config_input["tags"].update(tags)
        self._config_input["on_fail"] = on_fail
        self._func = func

    def config(self):
        self._has_tests = False

        while len(self._config_input["sources"]) > 0:
            self.src(self._config_input["sources"].pop())

        while len(self._config_input["outputs"]) > 0:
            self.out(self._config_input["outputs"].pop())

    def setup(self):
        pass

    def compile(self):
        pass

    def run(self):
        # Get the names of the arguments to the function
        sig = inspect.signature(self._func)

        wrapper_params = []
        for param in sig.parameters:
            if param == "context":
                # Special parameter equivalent to self
                wrapper_params.append(self)
            elif param in self.connections:
                if isinstance(self.connections[param], UnknownDb):
                    return self.fail(f'Connection "{param}" missing from settings')
                # The name of a connection makes it so that that argument
                # is linked to the connection object itself
                wrapper_params.append(self.connections[param])
            else:
                if param not in self.task_parameters:
                    value = None
                else:
                    value = self.task_parameters[param]
                # The rest are interpreted as task parameters
                wrapper_params.append(value)

        return self._func(*wrapper_params)

    def test(self):
        pass


class DecoratorTaskWrapper(Task):
    def __init__(
        self,
        func,
        sources: Optional[Union[List[str], str]] = None,
        outputs: Optional[Union[List[str], str]] = None,
        parents: Optional[Union[List[str], str]] = None,
        tags: Optional[Union[List[str], str]] = None,
        on_fail: Optional[OnFailValue] = None,
    ):
        """The init method collects the information provided by the decorator itself"""

        self.func = func

        # Get the names of the arguments to the function
        sig = inspect.signature(func)
        self.func_arguments = [p for p in sig.parameters]

        # Need to store these temporarily
        if sources is None:
            self.sources = set()
        elif isinstance(sources, str):
            self.sources = set()
            self.sources.add(sources)
        else:
            self.sources = sources

        if outputs is None:
            self.outputs = set()
        elif isinstance(outputs, str):
            self.outputs = set()
            self.outputs.add(outputs)
        else:
            self.outputs = outputs

        if parents is None:
            self.parents = set()
        elif isinstance(parents, str):
            self.parents = set()
            self.parents.add(parents)
        else:
            self.parents = parents

        if tags is None:
            self.tags = set()
        elif isinstance(tags, str):
            self.tags = set()
            self.tags.add(tags)
        else:
            self.tags = set(tags)

        if on_fail is None:
            self.on_fail = OnFailValue.skip
        elif on_fail in ("skip", "no_skip"):
            self.on_fail = OnFailValue[f"{on_fail}"]
        else:
            raise ValueError(f"{on_fail} not a valid value for on_fail field")

    def __call__(
        self,
        name,
        group,
        tracker,
        run_arguments,
        task_parameters,
        project_parameters,
        default_db,
        connections,
        compiler,
        src,
        out,
    ):
        task = DecoratorTask(
            name,
            group,
            tracker,
            run_arguments,
            task_parameters,
            project_parameters,
            default_db,
            connections,
            compiler,
            src,
            out,
            self.sources,
            self.outputs,
            self.parents,
            self.tags,
            self.on_fail,
            self.func,
        )

        return task


def task_type(func=None, sources=None, outputs=None, parents=None):
    if func:
        return DecoratorTaskWrapper(func)
    else:

        def wrapper(func):
            return DecoratorTaskWrapper(
                func,
                sources=sources,
                outputs=outputs,
                parents=parents,
            )

        return wrapper


def task(func=None, sources=None, outputs=None, parents=None, tags=None, on_fail=None):
    if func:
        return DecoratorTaskWrapper(func)
    else:

        def wrapper(func):
            return DecoratorTaskWrapper(
                func,
                sources=sources,
                outputs=outputs,
                parents=parents,
                tags=tags,
                on_fail=on_fail,
            )

        return wrapper
