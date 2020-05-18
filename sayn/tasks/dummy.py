import logging

from .task import Task


class DummyTask(Task):
    def setup(self):
        logging.debug("Setting up Dummy Task")
        return self.ready()

    def compile(self):
        logging.debug("DummyTask compiling")
        return self.success()

    def run(self):
        logging.debug("DummyTask running.")
        return self.success()
