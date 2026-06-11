from dataclasses import dataclass
from typing import NamedTuple, Optional

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
    allow_error: Optional[bool] = False
    query_sql: Optional[str] = None
    query_title: Optional[str] = None
    max_rows: int = 20

@dataclass(frozen=True)
class GeneralConfig:
    log_file: str
    path_log_summ: str
    process_name: str
    dev_mode: Optional[bool] = False

class ConfigTotal(NamedTuple):
    stages: list[StageConfig]
    general: GeneralConfig
