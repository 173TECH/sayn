import inspect

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
    def __init__(self, func, sources=None, outputs=None, parents=None):
        """The init method collects the information provided by the decorator itself"""

        self.func = func

        # TODO accept the parents
        # self.parents = set()

        # Need to store these temporarily
        self.temp_sources = sources
        self.temp_outputs = outputs
        self.temp_parents = parents

    def __call__(self, app, name, jinja_env, **kwargs):
        self.app = app
        self.name = name

        self.jinja_env = jinja_env.overlay()
        self.jinja_env.globals.update(
            src=self.src_macro,
            config=self.config_macro,
        )

        self.kwargs = kwargs

        return self

    def config(self):
        if isinstance(self.temp_outputs, str):
            self.out(self.temp_outputs)
        elif isinstance(self.temp_outputs, list):
            for output in self.temp_outputs:
                self.out(self.temp_output)
        del self.temp_outputs

        if isinstance(self.temp_sources, str):
            self.out(self.temp_sources)
        elif isinstance(self.temp_sources, list):
            for source in self.temp_sources:
                self.src(self.temp_source)
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
                # The rest are interpreted as task parameters
                self.wrapper_params.append(
                    self.jinja_env.from_string(self.kwargs["parameters"][param]).render(
                        task=self
                    )
                )

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
