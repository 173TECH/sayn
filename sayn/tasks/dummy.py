from .task import Task


class DummyTask(Task):
    def config(self):
        return self.success()

    def setup(self):
        return self.success()

    def compile(self):
        return self.success()

    def run(self):
        return self.success()

    def test(self):
        return self.success()
