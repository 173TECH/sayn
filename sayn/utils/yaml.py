from strictyaml import (
    Bool,
    Map,
    Str,
    Optional,
    Regex,
    Any,
    MapPattern,
    UniqueSeq,
    Seq,
    Enum,
    ScalarValidator,
    as_document,
    EmptyDict,
)
from strictyaml import YAMLValidationError as ValidationError
from ..utils.ui import UI


class Identifier(Regex):
    def __init__(self):
        self._regex = r"^[a-zA-Z0-9][-_a-zA-Z0-9]+$"
        self._matching_message = "when expecting a valid identifier string"


class NotEmptyStr(Regex):
    def __init__(self):
        self._regex = r".+"
        self._matching_message = "when expecting a non-empty string"


class CaseInsensitiveStr(Str):
    def validate_scalar(self, chunk):
        return chunk.contents.lower()


class CaseInsensitiveEnum(Enum):
    def __init__(self, restricted_to):
        self._item_validator = CaseInsensitiveStr()
        for i in restricted_to:
            assert isinstance(i, str), "restricted_to must contain strings"
        assert isinstance(
            self._item_validator, ScalarValidator
        ), "item validator must be scalar too"
        self._restricted_to = [i.lower() for i in restricted_to]


def load(path):
    if not path.is_file():
        UI()._spinner_error(f"No {path.name} found")
        return
    else:
        from strictyaml import load

        try:
            return load(path.read_text())
        except ValidationError as e:
            UI()._spinner_error(f"Error reading {path.name}")
            UI()._spinner_error(f"{e}")
            return
