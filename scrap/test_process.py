import time
import random


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
        print(self.something)
        time.sleep(1)
        print("Done")
        return 1
