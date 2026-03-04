from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class StageResult:
    ok: bool
    returncode: int
    stdout: str
    stderr: str
    duration_s: float
    fatal: bool
    error: Optional[str] = None

@dataclass(frozen=True)
class StageConfig:
    name: str
    command: str = ""
    stage_type: str = "command"
    run_as_bat: bool = False
    show_output: bool = False
    timeout: Optional[int] = None
    result_key: Optional[str] = None
    condicion: Optional[str] = None
    query_sql: Optional[str] = None
    query_title: Optional[str] = None
    max_rows: int = 20
