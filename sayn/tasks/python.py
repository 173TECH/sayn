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


class DecoratorTask(PythonTask):
    _temp_sources = set()
    _temp_outputs = set()
    _temp_parents = set()

    _func = None

    def config(self):
        self._has_tests = False

        for output in self._temp_outputs:
            self.out(output)
        # del self._temp_outputs

        for source in self._temp_sources:
            self.src(source)
        # del self._temp_sources

        # Get the names of the arguments to the function
        sig = inspect.signature(self._func)
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

    def compile(self):
        pass

    def run(self):
        return self._func(*self.wrapper_params)

    def test(self):
        pass


class DecoratorTaskWrapper(Task):
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
            self.temp_sources = set()
        elif isinstance(sources, str):
            self.temp_sources = set()
            self.temp_sources.add(sources)
        else:
            self.temp_sources = sources

        if outputs is None:
            self.temp_outputs = set()
        elif isinstance(outputs, str):
            self.temp_outputs = set()
            self.temp_outputs.add(outputs)
        else:
            self.temp_outputs = outputs

        if parents is None:
            self.temp_parents = set()
        elif isinstance(parents, str):
            self.temp_parents = set()
            self.temp_parents.add(parents)
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
        )

        for o in self.temp_sources:
            task._temp_sources.add(o)

        for o in self.temp_outputs:
            task._temp_outputs.add(o)

        for o in self.temp_parents:
            task._temp_parents.add(o)

        task._func = self.func

        return task


def task_type(func=None, sources=None, outputs=None, parents=None):
    if func:
        return DecoratorTaskWrapper(func)
    else:

        def wrapper(func):
            return DecoratorTaskWrapper(
                func, sources=sources, outputs=outputs, parents=parents
            )

        return wrapper


def task(func=None, sources=None, outputs=None, parents=None):
    if func:
        return DecoratorTaskWrapper(func)
    else:

        def wrapper(func):
            return DecoratorTaskWrapper(
                func, sources=sources, outputs=outputs, parents=parents
            )

        return wrapper
