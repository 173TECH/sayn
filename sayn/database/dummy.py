from . import Database

# This class should only be used for allowing missing credentials


class Dummy(Database):
    def create_engine(self, settings):
        pass
