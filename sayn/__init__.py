# from importlib.metadata import version
#
# try:
#     __version__ = version(__name__)
# except:
#     pass

__version__ = "0.6.0"


from .tasks.python import PythonTask, task
