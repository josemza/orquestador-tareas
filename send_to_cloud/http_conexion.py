
import os
import json
import time
import socket
import uuid
import logging
import datetime
from html import unescape
from urllib.parse import urlparse
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = load_dotenv(os.path.join(BASE_DIR, '.env'))

url_flow = os.getenv("URL_FLOW")
path_log = os.getenv("PATH_LOG")

class JsonlFormatter(logging.Formatter):
    """
    Emite cada registro como una línea JSON.
    Incluye campos estándar y 'extra' si fueron provistos.
    """
    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z"),   #utcfromtimestamp(record.created).isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        # Campos extra comunes que solemos enviar en 'extra'
        for key in (
            "event", "host", "attempt", "attempts", "dns_ms",
            "corr_id", "status_code", "payload_len"
        ):
            if hasattr(record, key):
                obj[key] = getattr(record, key)
        return json.dumps(obj, ensure_ascii=False)

def setup_jsonl_logger(
    logger_name: str = "pa_sender",
    jsonl_path: str = os.path.join("logs", "pa_sender.jsonl"),
    level: int = logging.INFO
) -> logging.Logger:
    """
    Configura el logger con salida JSONL en archivo y JSON a consola.
    """
    # Asegurar carpeta
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()  # evita handlers duplicados si se llama más de una vez

    fmt = JsonlFormatter()

    # Archivo (JSONL) con rotación simple opcional
    try:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(jsonl_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    except Exception:
        # Fallback si hay entornos restringidos
        fh = logging.FileHandler(jsonl_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

# Inicializa el logger JSONL
logger = setup_jsonl_logger(jsonl_path=path_log)

def resolve_with_metrics(host: str, max_attempts: int = 8, base_delay: float = 1.0):
    """
    Intentos repetidos de resolución DNS con backoff exponencial.
    Devuelve (ok: bool, attempts: int, dns_ms: float)
    """
    attempts, delay = 0, base_delay
    start = time.perf_counter()
    while attempts < max_attempts:
        attempts += 1
        try:
            addrs = socket.getaddrinfo(host, 443)  # [(family, type, proto, canonname, sockaddr), ...]
            took_ms = (time.perf_counter() - start) * 1000
            logger.info(
                f"DNS OK '{host}'",
                extra={"event": "dns_ok", "host": host, "attempts": attempts, "dns_ms": round(took_ms, 1)}
            )
            return True, attempts, took_ms
        except socket.gaierror as e:
            logger.warning(
                f"DNS no resuelve '{host}': {e}",
                extra={"event": "dns_fail", "host": host, "attempt": attempts}
            )
            time.sleep(delay)
            delay = min(delay * 2, 10)
    took_ms = (time.perf_counter() - start) * 1000
    logger.error(
        f"DNS sin resolver para '{host}' tras {attempts} intentos",
        extra={"event": "dns_exhausted", "host": host, "attempts": attempts, "dns_ms": round(took_ms, 1)}
    )
    return False, attempts, took_ms

def make_session(trust_env: bool = False) -> requests.Session:
    """
    Crea una sesión requests con reintentos para POST (5xx/429 y errores de conexión).
    """
    s = requests.Session()
    s.trust_env = trust_env
    retries = Retry(
        total=5, connect=5, status=5, read=0,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"POST"},
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def post_to_flow(payload: dict, flow_url: str = url_flow, headers=None, trust_env: bool = False, dns_attempts: int = 8):
    """
    Envía un POST JSON al trigger de Power Automate/Logic Apps.
    Regresa (resp: Response, corr_id: str)
    """
    url = unescape(flow_url)
    host = urlparse(url).hostname

    ok, tries, dns_ms = resolve_with_metrics(host, max_attempts=dns_attempts)
    if not ok:
        raise RuntimeError(f"DNS sin resolver para '{host}' tras {tries} intentos ({dns_ms:.0f} ms)")

    session = make_session(trust_env=trust_env)
    corr_id = str(uuid.uuid4())

    # LoggerAdapter para inyectar corr_id en todos los siguientes logs
    adapter = logging.LoggerAdapter(logger, {"corr_id": corr_id})

    hdrs = {
        "Content-Type": "application/json",
        "x-correlation-id": corr_id,     # útil para rastrear en el flujo
    }
    if headers:
        hdrs.update(headers)

    payload_len = len(str(payload))

    adapter.info(
        f"POST -> {host}",
        extra={"event": "http_post", "host": host, "payload_len": payload_len}
    )
    resp = session.post(url, json=payload, headers=hdrs, timeout=(5, 30))

    adapter.info(
        f"Respuesta: {resp.status_code}",
        extra={"event": "http_resp", "host": host, "status_code": resp.status_code}
    )
    resp.raise_for_status()
    return resp, corr_id