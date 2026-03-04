from configparser import ConfigParser

from orquestador.models import StageConfig

def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    key = value.strip().lower()
    values_map = {
        "true": True, "1": True, "yes": True, "y": True,
        "false": False, "0": False, "no": False, "n": False,
    }
    return values_map.get(key, default)

def build_stage(config: ConfigParser, section: str) -> StageConfig:
    stage_name = section
    stage_type = config.get(section, "type", fallback="command").strip().lower()
    if stage_type not in {"command", "query"}:
        stage_type = "command"
    command = config.get(section, "command", fallback="") if stage_type == "query" else config.get(section, "command")
    timeout = config.getint(section, "timeout", fallback=None)
    result_key = config.get(section, "result_key", fallback=None)
    condicion = config.get(section, "condicion", fallback=None)
    run_as_bat = parse_bool(config.get(section, "run_as_bat", fallback="false"))
    show_output = parse_bool(config.get(section, "show_output", fallback="false"))
    query_sql = config.get(section, "query_sql", fallback=None)
    query_title = config.get(section, "query_title", fallback=stage_name)
    max_rows = config.getint(section, "max_rows", fallback=20)
    if max_rows < 1:
        max_rows = 1
    if max_rows > 20:
        max_rows = 20

    return StageConfig(
        name=stage_name,
        command=command,
        stage_type=stage_type,
        run_as_bat=run_as_bat,
        show_output=show_output,
        timeout=timeout,
        result_key=result_key,
        condicion=condicion,
        query_sql=query_sql,
        query_title=query_title,
        max_rows=max_rows,
    )
