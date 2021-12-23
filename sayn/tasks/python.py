from .task import Task


class PythonTask(Task):
    def config(self):
        self.debug("Nothing to be done")
        return self.success()

    def setup(self):
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
