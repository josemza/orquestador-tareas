# app/db/engine.py
from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # carpeta raíz del proyecto

env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

def _try_external_engine() -> Optional[Engine]:
    """
    Si existe el módulo corporativo, úsalo (Oracle en el trabajo).
    Debe exponer: from conexion.conexion import get_engine
    """
    try:
        from conexion.conexion import get_engine as external_get_engine  # type: ignore
        eng = external_get_engine()
        return eng
    except Exception:
        return None


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Orden de prioridad:
    1) engine del módulo corporativo (si existe)
    2) DB_URL (para pruebas: mariadb/mysql/sqlite/postgres/oracle, etc.)
    """
    eng = _try_external_engine()
    if eng is not None:
        return eng

    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError(
            "No hay conexión configurada. Define DB_URL (ej: mariadb+pymysql://user:pass@host:3306/db) "
            "o asegúrate de que exista conexion.conexion.get_engine()."
        )

    return create_engine(db_url, pool_pre_ping=True, future=True)