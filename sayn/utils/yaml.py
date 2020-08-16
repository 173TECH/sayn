# TODO Module using strictyaml. To be removed once dbs are migrated to pydantic

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
