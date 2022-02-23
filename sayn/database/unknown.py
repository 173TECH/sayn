from . import Database

# This class should only be used for allowing missing credentials


class UnknownDb(Database):
    def create_engine(self, settings):
        pass
