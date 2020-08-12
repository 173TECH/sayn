from .task import TaskRunner


class PythonTask(TaskRunner):
    def setup(self):
        self.logger.debug("Setting up Python Task")
        return self.ready()

    def compile(self):
        self.logger.debug("PythonTask compiling")
        return self.success()

    def run(self):
        self.logger.debug("PythonTask running.")
        return self.success()
