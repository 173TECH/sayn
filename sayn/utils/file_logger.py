import logging
from pathlib import Path


class FileLogger:
    def __init__(self, run_id=None, debug=False, log_file=Path("logs/sayn.log")):
        self.run_id = run_id
        self.debug = debug
        self.level = logging.DEBUG if debug else logging.INFO
        logging.getLogger().setLevel(self.level)
        self.log_file = log_file
        self.sayn_project = Path().cwd().parts[-1]

        self.progress = None
        self.task_name = None
        self.stage_name = None

        self.set_file_logger(log_file)

    def set_config(self, **kwargs):
        if "stage_name" in kwargs:
            self.stage_name = kwargs["stage_name"]

        if "task_name" in kwargs:
            self.task_name = kwargs["task_name"]

        if "progress" in kwargs:
            self.progress = kwargs["progress"]

        self.set_file_formatter()

    def set_debug(self):
        self.level = logging.DEBUG
        logging.getLogger().setLevel(self.level)
        if self.file_handler is not None:
            self.file_handler.setLevel(self.level)

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

            self.set_file_formatter()
            logging.getLogger().addHandler(self.file_handler)

    def set_file_formatter(self):
        if self.file_handler is not None:
            fmt_string = f"{self.run_id}|{self.sayn_project}|" + "%(asctime)s|"

            if self.stage_name is not None:
                fmt_string += f"{self.stage_name}|"

            if self.progress is not None:
                fmt_string += f"{self.progress}|"

            fmt_string += "%(levelname)s|"

            if self.task_name is not None:
                fmt_string += f"{self.task_name}|"

            fmt_string += "%(message)s"

            formatter = logging.Formatter(fmt_string, "%Y-%m-%d %H:%M:%S",)
            self.file_handler.setFormatter(formatter)

    def set_formatters(self):
        self.set_file_formatter()
