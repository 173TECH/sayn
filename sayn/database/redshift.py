from .postgresql import Postgresql


class Redshift(Postgresql): pass
#     def __init__(self, name, name_in_settings, settings):
#         # self.dialect = "postgresql"
#         # connection_details = settings
#         # connection_details.pop("type")
#         super().__init__(name, name_in_settings, connection_details)
