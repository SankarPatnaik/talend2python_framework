"""Runtime helpers for generated Talend pipelines."""

try:
    from .components import (
        TMysqlConnection,
        TMysqlInput,
        TMysqlOutput,
        TFileInputDelimited,
        TFileInputExcel,
        TFileList,
        TFileArchive,
        TRowGenerator,
        TMsgBox,
        TLogRow,
        TPreJob,
        TMap,
        TJoin,
        TJava,
        TRunJob,
    )
except Exception:  # pragma: no cover - optional deps like pandas may be missing
    TMysqlConnection = TMysqlInput = TMysqlOutput = None
    TFileInputDelimited = TFileInputExcel = TFileList = None
    TFileArchive = TRowGenerator = TMsgBox = None
    TLogRow = TPreJob = TMap = TJoin = None
    TJava = TRunJob = None
from .utils import Talend2PyError, handle_component_error, safe_call
from .routines import register, routine, registry

__all__ = [
    "TMysqlConnection",
    "TMysqlInput",
    "TMysqlOutput",
    "TFileInputDelimited",
    "TFileInputExcel",
    "TFileList",
    "TFileArchive",
    "TRowGenerator",
    "TMsgBox",
    "TLogRow",
    "TPreJob",
    "TMap",
    "TJoin",
    "TJava",
    "TRunJob",
    "Talend2PyError",
    "handle_component_error",
    "safe_call",
    "register",
    "routine",
    "registry",
]
