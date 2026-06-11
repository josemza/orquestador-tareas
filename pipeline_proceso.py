# -*- coding: utf-8 -*-
"""
fecha: 30/01/2025
version: 9.0
Cambios:
    - Se agrega registro de tiempos por etapa.
    - Se modulariza el script para mejor mantenimiento

@author: Jose Zuniga
"""

import argparse
import sys
import uuid


from conexion.conexion import get_engine
from orquestador.paths import get_unc_path
from orquestador.config_loader import arg_parser
from orquestador.logging_setup import setup_logging
from orquestador.pipeline import Pipeline
from orquestador.utils import parse_extra_args


def main():
    #---- Parsear argumentos del BAT ----#

    parser = argparse.ArgumentParser(description="Pipeline para automatizar procesos en GCI")
    parser.add_argument('--config', help="Ruta del archivo de configuracion", default="proceso.ini")
    parser.add_argument('--batpath', type=str, help="Ruta completa del archivo .bat que ejecuto este script", default=None)
    args, unknown = parser.parse_known_args()

    #---- Parsear argumentos extraen un diccionario ----#
    
    extra_params = parse_extra_args(unknown)
    
    #---- Configurar el proceso ----#

    engine = get_engine() # Establecer la conexion con la BBDD
    commands,general_config = arg_parser(args) # Levantar las configuraciones
    logger = setup_logging(general_config.log_file)
    bat_path = get_unc_path(args.batpath) if args.batpath else None # Obtener la ruta UNC del bat en caso llegue como ruta mapeada X:, Z:, etc
    ejecucion_id = str(uuid.uuid4())

    #---- Ejecutar el Pipeline configurado ----#

    pipeline = Pipeline(
        commands=commands,
        engine=engine,
        general_config=general_config,
        bat_path=bat_path,
        ejecucion_id=ejecucion_id,
        initial_params=extra_params,
        )
    
    success = pipeline.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
