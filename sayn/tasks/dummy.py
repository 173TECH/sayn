from .task import Task
from ..utils.ui import UI


class DummyTask(Task):
    def setup(self):
        UI()._debug("Setting up Dummy Task")
        return self.ready()

    def compile(self):
        UI()._debug("DummyTask compiling")
        return self.success()

    def run(self):
        UI()._debug("DummyTask running.")
        return self.success()
