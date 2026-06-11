from typing import Any

def parse_extra_args(unknow_args: list) -> dict[str,Any]:
    params = {}
    i = 0
    while i < len(unknow_args):
        key = unknow_args[i]
        if key.startswith("--"):
            key = key.lstrip("-")
            # Verificar si el siguiente elemento es un valor o otra flag
            if i + 1 < len(unknow_args) and not unknow_args[i + 1].startswith("--"):
                val = unknow_args[i + 1]
                i += 2
            else:
                val = "true" # Flag booleana sin valor
                i += 1
            params[key] = val
        else:
            i += 1
    return params
