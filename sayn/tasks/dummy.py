from .task import Task
from ..utils.ui import UI


class DummyTask(Task):
    def setup(self):
        UI().debug("Setting up Dummy Task")
        return self.ready()

    def compile(self):
        UI().debug("DummyTask compiling")
        return self.success()

    def run(self):
        UI().debug("DummyTask running.")
        return self.success()
