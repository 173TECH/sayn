from . import Task


class DummyTask(Task):
    def setup(self):
        return self.success()

    def compile(self):
        return self.success()

    def run(self):
        return self.success()
