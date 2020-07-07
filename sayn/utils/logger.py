import logging
import coloredlogs
from pathlib import Path

from .singleton import singleton


@singleton
class Logger:
    def __init__(self, run_id=None, debug=False, log_file=Path("logs/sayn.log")):
        self.run_id = run_id
        self.level = logging.DEBUG if debug else logging.INFO
        logging.getLogger().setLevel(self.level)
        self.log_file = log_file
        self.sayn_project = Path().cwd().parts[-1]

        self.progress = None
        self.task_name = None
        self.stage_name = None

        self.set_console_logger()
        self.set_file_logger(log_file)

    def set_config(self, **kwargs):
        if "stage" in kwargs:
            self.stage_name = kwargs["stage"]

        if "task" in kwargs:
            self.task_name = kwargs["task"]

        if "progress" in kwargs:
            self.progress = kwargs["progress"]

        self._set_console_formatter()
        self._set_file_formatter()

    def set_debug(self):
        self.level = logging.DEBUG
        logging.getLogger().setLevel(self.level)
        self.console_handler.setLevel(self.level)
        if self.file_handler is not None:
            self.file_handler.setLevel(self.level)

    def set_console_logger(self):
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(self.level)
        self._set_console_formatter()
        logging.getLogger().addHandler(self.console_handler)

    def set_file_logger(self, log_file=None):
        if log_file is not None:
            self.log_file = log_file

        if self.log_file is None:
            self.file_handler = None
        else:
            # Create folder if it doesn't exists
            if not self.log_file.parent.exists():
                self.log_file.parent.mkdir(parents=True)

            self.file_handler = logging.FileHandler(self.log_file)
            self.file_handler.setLevel(self.level)

            self._set_file_formatter()
            logging.getLogger().addHandler(self.file_handler)

    def _set_console_formatter(self):
        fmt_string = "[%(asctime)s] "

        if self.stage_name is not None:
            fmt_string += f"{self.stage_name}|"

        if self.progress is not None:
            fmt_string += f"{self.progress}%%|"

        fmt_string += "%(levelname)s|"

        if self.task_name is not None:
            fmt_string += f"{self.task_name}|"

        fmt_string += "%(message)s"

        formatter = coloredlogs.ColoredFormatter(
            fmt=fmt_string,
            field_styles={
                #    "asctime": {}
                #    "stage_name": {"bold": True, "color": "blue"},
                #    "progress": {"bold": True, "color": "blue"},
            },
            level_styles={
                "debug": {"color": "blue"},
                "info": {},
                "warning": {"color": "yellow"},
                "error": {"color": "red"},
                "critical": {"color": "red"},
                # "exception": {"color": "red"},
            },
        )
        self.console_handler.setFormatter(formatter)

    def _set_file_formatter(self):
        if self.file_handler is not None:
            fmt_string = f"{self.run_id}|{self.sayn_project}|" + "%(asctime)s|"

            if self.stage_name is not None:
                fmt_string += f"{self.stage_name}|"

            if self.progress is not None:
                fmt_string += f"{self.progress}%%|"

            fmt_string += "%(levelname)s|"

            if self.task_name is not None:
                fmt_string += f"{self.task_name}|"

            fmt_string += "%(message)s"

            formatter = logging.Formatter(fmt_string, "%Y-%m-%d %H:%M:%S",)
            self.file_handler.setFormatter(formatter)

    def _set_formatters(self):
        self._set_console_formatter()
        self._set_file_formatter()
