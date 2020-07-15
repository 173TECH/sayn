from .task import Task
from ..utils.ui import UI


class IgnoreTask(Task):
    def should_run(self):
        return False

    def setup(self):
        UI().debug("Setting up IgnoreTask")
        return self.ready()

    def compile(self):
        UI().debug("IgnoreTask compiling")
        return self.success()

    def run(self):
        UI().debug("IgnoreTask running.")
        return self.success()
