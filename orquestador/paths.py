from hashlib import sha256
import logging
import os
import win32wnet

from orquestador.constants import DEFAULT_ENCODING

def get_unc_path(path: str) -> str:
    # Asegura que la ruta este con doble backslash o raw string
    path = os.path.abspath(path)
    
    # Si ya es UNC no hacemos nada
    if path.startswith('\\\\'):
        return str.upper(path)
    
    try:
        unc_path = win32wnet.WNetGetUniversalName(path)
        # unc_path puede ser una cadena o una tupla dependiendo del sistema
        if isinstance(unc_path, dict) and 'remote_name' in unc_path:
            return str.upper(unc_path['remote_name'] + path[2:])
        elif isinstance(unc_path, str):
            return str.upper(unc_path)
        else:
            return str.upper(path)  # No se pudo convertir, se devuelve tal cual
    except Exception as e:
        logging.warning(f"No se pudo resolver la ruta UNC: {e}")
        return str.upper(path)

def fingerprint_from_norm_path(bat_path_norm: str) -> str:

    if not isinstance(bat_path_norm, str) or not bat_path_norm.strip():
        raise ValueError("bat_path_norm debe ser un string no vacío ya normalizado.")

    # No modificamos la ruta: confiamos en que ya viene normalizada.
    return sha256(bat_path_norm.encode(DEFAULT_ENCODING)).hexdigest()
