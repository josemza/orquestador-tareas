from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CRITICAL_KEYS = {
    "request_id",
    "usuario",
    "solicitud_id",
    "reporte_codigo",
    "modo_inputs",
}

ROOT_CONTEXT_KEYS = (
    "request_id",
    "solicitud_id",
    "reporte_codigo",
    "usuario",
    "modo_inputs",
)

SUPPORTED_INPUT_TYPES = {"archivo", "periodo", "texto"}


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _normalize_key(raw_key: Any, source: str) -> str:
    key = str(raw_key).strip()
    if not key:
        raise ValueError(f"Se encontró una clave vacía en {source}.")
    return key


def _ensure_object(value: Any, section_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"'{section_name}' debe ser un objeto JSON.")
    return value


def _set_context_value(context: dict[str, str], key: Any, value: Any, source: str) -> None:
    if not _has_value(value):
        return

    normalized_key = _normalize_key(key, source)
    normalized_value = _stringify(value)

    if normalized_key in context:
        if context[normalized_key] == normalized_value:
            return
        raise ValueError(
            f"Conflicto dentro del archivo de --params para la clave '{normalized_key}': "
            f"'{context[normalized_key]}' vs '{normalized_value}' ({source})."
        )

    context[normalized_key] = normalized_value


def load_params_file(path: str) -> dict[str, Any]:
    params_path = Path(path)
    if not params_path.is_file():
        raise FileNotFoundError(f"El archivo indicado en --params no existe: {path}")

    try:
        with params_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "El archivo indicado en --params no contiene JSON válido: "
            f"{path} (línea {exc.lineno}, columna {exc.colno})."
        ) from exc
    except OSError as exc:
        raise ValueError(f"No se pudo leer el archivo indicado en --params: {path}. {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("El JSON de --params debe tener un objeto en la raíz.")

    return payload


def _flatten_file_input(name: str, input_data: dict[str, Any], context: dict[str, str]) -> None:
    value = input_data.get("valor")
    path_value = input_data.get("ruta_archivo")

    if not _has_value(value) and not _has_value(path_value):
        raise ValueError(
            f"El input '{name}' no tiene 'valor' y tampoco 'ruta_archivo'."
        )

    metadata = input_data.get("metadata", {})
    if "metadata" in input_data:
        metadata = _ensure_object(metadata, f"inputs.{name}.metadata")

    resolved_value = value if _has_value(value) else path_value
    resolved_path = path_value if _has_value(path_value) else value

    _set_context_value(context, name, resolved_value, f"inputs.{name}")
    _set_context_value(context, f"{name}_ruta", resolved_path, f"inputs.{name}")
    _set_context_value(context, f"{name}_nombre", metadata.get("nombre_archivo"), f"inputs.{name}.metadata")
    _set_context_value(context, f"{name}_extension", metadata.get("extension"), f"inputs.{name}.metadata")


def _flatten_period_input(name: str, input_data: dict[str, Any], context: dict[str, str]) -> None:
    value = input_data.get("valor")
    if not _has_value(value):
        raise ValueError(f"El input '{name}' no tiene 'valor'.")

    metadata = input_data.get("metadata", {})
    if "metadata" in input_data:
        metadata = _ensure_object(metadata, f"inputs.{name}.metadata")

    period_value = _stringify(value)
    year_value = metadata.get("anio")
    month_value = metadata.get("mes")

    if not _has_value(year_value):
        if len(period_value) < 4:
            raise ValueError(
                f"El input '{name}' no tiene metadata.anio y su valor no permite derivarlo: '{period_value}'."
            )
        year_value = period_value[:4]

    if not _has_value(month_value):
        if len(period_value) < 6:
            raise ValueError(
                f"El input '{name}' no tiene metadata.mes y su valor no permite derivarlo: '{period_value}'."
            )
        month_value = period_value[4:6]

    _set_context_value(context, name, period_value, f"inputs.{name}")
    _set_context_value(context, f"{name}_anio", year_value, f"inputs.{name}")
    _set_context_value(context, f"{name}_mes", _stringify(month_value).zfill(2), f"inputs.{name}")


def _flatten_text_input(name: str, input_data: dict[str, Any], context: dict[str, str]) -> None:
    value = input_data.get("valor")
    if not _has_value(value):
        raise ValueError(f"El input '{name}' no tiene 'valor'.")

    _set_context_value(context, name, value, f"inputs.{name}")


def flatten_params_payload(payload: dict[str, Any]) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise ValueError("El payload de --params debe ser un objeto.")

    context: dict[str, str] = {}

    for key in ROOT_CONTEXT_KEYS:
        _set_context_value(context, key, payload.get(key), "raíz")

    if "metadata" in payload:
        metadata = _ensure_object(payload["metadata"], "metadata")
        for key, value in metadata.items():
            _set_context_value(context, key, value, "metadata")

    if "parametros" in payload:
        parametros = _ensure_object(payload["parametros"], "parametros")
        for key, value in parametros.items():
            _set_context_value(context, key, value, "parametros")

    if "inputs" in payload:
        inputs = _ensure_object(payload["inputs"], "inputs")
        for raw_name, raw_input in inputs.items():
            input_name = _normalize_key(raw_name, "inputs")
            if not isinstance(raw_input, dict):
                raise ValueError(f"El input '{input_name}' debe ser un objeto JSON.")

            input_type = raw_input.get("tipo")
            if not _has_value(input_type):
                raise ValueError(f"El input '{input_name}' no tiene 'tipo'.")

            input_type = _stringify(input_type).strip().lower()
            if input_type not in SUPPORTED_INPUT_TYPES:
                allowed_types = ", ".join(sorted(SUPPORTED_INPUT_TYPES))
                raise ValueError(
                    f"El input '{input_name}' tiene un tipo no soportado: '{input_type}'. "
                    f"Tipos soportados: {allowed_types}."
                )

            if input_type == "archivo":
                _flatten_file_input(input_name, raw_input, context)
            elif input_type == "periodo":
                _flatten_period_input(input_name, raw_input, context)
            else:
                _flatten_text_input(input_name, raw_input, context)

    return context


def build_context_from_params_file(path: str) -> dict[str, str]:
    payload = load_params_file(path)
    return flatten_params_payload(payload)


def merge_contexts(cli_context: dict[str, Any], json_context: dict[str, Any]) -> dict[str, str]:
    merged = {_normalize_key(key, "CLI"): _stringify(value) for key, value in cli_context.items()}

    for raw_key, raw_value in json_context.items():
        key = _normalize_key(raw_key, "JSON")
        value = _stringify(raw_value)

        if key not in merged:
            merged[key] = value
            continue

        if key in CRITICAL_KEYS:
            if merged[key] == value:
                continue
            raise ValueError(
                f"Conflicto en clave crítica '{key}': CLI='{merged[key]}' JSON='{value}'."
            )

        if merged[key] == value:
            continue

        raise ValueError(
            f"Conflicto de contexto para la clave '{key}': "
            f"CLI='{merged[key]}' JSON='{value}'."
        )

    return merged
