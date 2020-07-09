import logging

from .task import Task
from ..utils.ui import UI


class PythonTask(Task):
    def __init__(self, name, task):
        super(PythonTask, self).__init__(name, task)

        # Add some convenient properties
        self.default_db = self.sayn_config.default_db

        #Add UI
        self.ui = UI()

    def setup(self):
        logging.debug("Setting up Python Task")
        return self.ready()

    def compile(self):
        logging.debug("PythonTask compiling")
        return self.success()

    def run(self):
        logging.debug("PythonTask running.")
        return self.success()
