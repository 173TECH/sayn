import inspect
from typing import Optional, List, Union

from .task import Task


class PythonTask(Task):
    def config(self, **task_config):
        self.debug("Nothing to be done")
        return self.success()

    def setup(self, needs_recompile: bool):
        self.debug("Nothing to be done")
        return self.success()

    def run(self):
        self.debug("Nothing to be done")
        return self.success()

    def compile(self):
        self.debug("Nothing to be done")
        return self.success()

    def test(self):
        self.debug("Nothing to be done")
        return self.success()


class DecoratorTask(Task):
    def __init__(
        self,
        func,
        sources: Optional[Union[List[str], str]] = None,
        outputs: Optional[Union[List[str], str]] = None,
        parents: Optional[Union[List[str], str]] = None,
    ):
        """The init method collects the information provided by the decorator itself"""

        self.func = func

        # TODO accept the parents
        # self.parents = set()

        # Need to store these temporarily
        if sources is None:
            self.temp_sources = list()
        else:
            self.temp_sources = sources

        if outputs is None:
            self.temp_outputs = list()
        else:
            self.temp_outputs = outputs

        if parents is None:
            self.temp_parents = list()
        else:
            self.temp_parents = parents

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
        self.name = name
        self.group = group
        self._tracker = tracker
        self.run_arguments = run_arguments
        self.task_parameters = task_parameters
        self.project_parameters = project_parameters
        self._default_db = default_db
        self.connections = connections
        self.compiler = compiler
        self.src = src
        self.out = out

        return self

    def config(self):
        if isinstance(self.temp_outputs, str):
            self.out(self.temp_outputs)
        elif isinstance(self.temp_outputs, list):
            for output in self.temp_outputs:
                self.out(output)
        del self.temp_outputs

        if isinstance(self.temp_sources, str):
            self.out(self.temp_sources)
        elif isinstance(self.temp_sources, list):
            for source in self.temp_sources:
                self.src(source)
        del self.temp_sources

        # Get the names of the arguments to the function
        sig = inspect.signature(self.func)
        self.wrapper_params = []
        for param in sig.parameters:
            if param == "context":
                # Special parameter equivalent to self
                self.wrapper_params.append(self)
            elif param in self.connections:
                # The name of a connection makes it so that that argument
                # is linked to the connection object itself
                self.wrapper_params.append(self.connections[param])
            else:
                if param not in self.task_parameters:
                    value = None
                else:
                    value = self.task_parameters[param]
                # The rest are interpreted as task parameters
                self.wrapper_params.append(value)

    def setup(self, needs_update):
        pass

    def run(self):
        return self.func(*self.wrapper_params)


def task_type(func=None, sources=None, outputs=None, parents=None):
    if func:
        return DecoratorTask(func)
    else:

        def wrapper(func):
            return DecoratorTask(
                func, sources=sources, outputs=outputs, parents=parents
            )

        return wrapper


def task(func=None, sources=None, outputs=None, parents=None):
    if func:
        return DecoratorTask(func)
    else:

        def wrapper(func):
            return DecoratorTask(
                func, sources=sources, outputs=outputs, parents=parents
            )

        return wrapper
