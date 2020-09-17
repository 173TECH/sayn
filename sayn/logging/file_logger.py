from pathlib import Path
import logging

from .logger import Logger
from .log_formatter import LogFormatter


class FileLogger(Logger):
    fmt = LogFormatter(use_colour=False, output_ts=False)
    logger = None

    def __init__(self, folder, format=None):
        if format is None:
            format = ("%(asctime)s|%(levelname)s|%(message)s",)

        formatter = logging.Formatter(format)

        log_file = Path(folder, "sayn.log")
        if not log_file.parent.exists():
            log_file.parent.mkdir(parents=True)

        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)

        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        self.logger = logger

    def print(self, s=None):
        if s is not None:
            if s["level"] == "info":
                func = self.logger.info
            elif s["level"] == "error":
                func = self.logger.error
            elif s["level"] == "warning":
                func = self.logger.warning
            else:
                func = self.logger.debug
            s = s["message"]

            if isinstance(s, str):
                s = [s]
            elif not isinstance(s, list):
                raise ValueError("error in logging print")

            func(f"{s[0]}")
            for e in s[1:]:
                for l in e.split("\n"):
                    func(f"{l}")
