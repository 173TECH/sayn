from .logger import Logger
from .log_formatter import LogFormatter


class ConsoleLogger(Logger):
    fmt = LogFormatter(use_colour=True, output_ts=True)
    is_debug = True

    def __init__(self, debug=True):
        self.is_debug = debug

    def print(self, s=None):
        if s is None:
            print()
        else:
            prefix = "  " * self.current_indent
            s = s["message"]
            if isinstance(s, str):
                s = [s]
            elif not isinstance(s, list):
                raise ValueError("error in logging print")

            print(f"{prefix}{s[0]}")
            for e in s[1:]:
                for l in e.split("\n"):
                    print(f"{prefix}  {l}")
