import logging

from .task import Task


class PythonTask(Task):
    def __init__(self, name, task, group, model):
        super(PythonTask, self).__init__(name, task, group, model)
        self.default_db = self.sayn_config.default_db

    def setup(self):
        logging.debug("Setting up Python Task")
        return self.ready()

    def compile(self):
        logging.debug("PythonTask compiling")
        return self.finished()

    def run(self):
        logging.debug("PythonTask running.")
        return self.finished()
