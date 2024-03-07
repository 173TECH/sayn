from multiprocessing import Process

import time
import random

# Both implementations need to activate the connection in their Run method.


class Task:
    def __init__(self):
        self.id = ""
        self.something = ""
        self.connections = dict()

    def setup(self, id, something, connections):
        self.id = id
        self.something = something
        self.connections = connections

    def run(self):
        print("Starting work!")
        time.sleep(random.randint(2, 5))
        for c in self.connections.values():
            c._activate_connection()

        result = self.connections["target_db"].read_data("SELECT 1")
        print(result)
        print(self.something)
        time.sleep(1)
        print("Done")
        return 1


class TaskProcess(Process):
    def __init__(self, id, something, connections):
        Process.__init__(self)
        self.id = id
        self.something = something
        self.connections = connections

        # I thought this could work here, but it doesn't
        # for c in self.connections.values():
        #     c._activate_connection()

    def setup(self):
        return

    def run(self):
        print("Starting work!")
        time.sleep(random.randint(2, 5))

        for c in self.connections.values():
            c._activate_connection()

        result = self.connections["target_db"].read_data("SELECT 1")
        print(result)
        print(self.something)
        time.sleep(1)
        print("Done")
        return 1
