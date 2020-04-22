import logging

from .task import Task


class IgnoreTask(Task):
    def should_run(self):
        return False

    def setup(self):
        logging.debug("Setting up IgnoreTask")
        return self.ready()

    def compile(self):
        logging.debug("IgnoreTask compiling")
        return self.finished()

    def run(self):
        logging.debug("IgnoreTask running.")
        return self.finished()
