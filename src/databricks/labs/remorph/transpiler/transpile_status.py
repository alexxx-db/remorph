<<<<<<< HEAD
import abc
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TranspileError(abc.ABC):
    file_path: Path
    error_msg: str

    def __str__(self):
        return f"{type(self).__name__}(file_path='{self.file_path!s}', error_msg='{self.error_msg}')"


@dataclass
class ParserError(TranspileError):
    pass


@dataclass
class ValidationError(TranspileError):
    pass
=======
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


# not using StrEnum because they only appear with Python 3.11
class ErrorSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class ErrorKind(Enum):
    ANALYSIS = "ANALYSIS"
    PARSING = "PARSING"
    GENERATION = "GENERATION"
    VALIDATION = "VALIDATION"
    INTERNAL = "INTERNAL"


@dataclass
class CodePosition:
    line: int  # 0-based line number
    character: int  # 0-based character number


@dataclass
class CodeRange:
    start: CodePosition
    end: CodePosition


@dataclass
class TranspileError:
    code: str
    kind: ErrorKind
    severity: ErrorSeverity
    path: Path
    message: str
    range: CodeRange | None = None

    def __str__(self):
        return f"{type(self).__name__}(code={self.code}, kind={self.kind.name}, severity={self.severity.name}, path='{self.path!s}', message='{self.message}')"
>>>>>>> databrickslabs-main


@dataclass
class TranspileStatus:
    file_list: list[Path]
    no_of_transpiled_queries: int
<<<<<<< HEAD
    parse_error_count: int
    validate_error_count: int
    error_list: list[TranspileError]
=======
    error_list: list[TranspileError]

    @property
    def analysis_error_count(self) -> int:
        return len([error for error in self.error_list if error.kind == ErrorKind.ANALYSIS])

    @property
    def parsing_error_count(self) -> int:
        return len([error for error in self.error_list if error.kind == ErrorKind.PARSING])

    @property
    def generation_error_count(self) -> int:
        return len([error for error in self.error_list if error.kind == ErrorKind.GENERATION])

    @property
    def validation_error_count(self) -> int:
        return len([error for error in self.error_list if error.kind == ErrorKind.VALIDATION])
>>>>>>> databrickslabs-main
