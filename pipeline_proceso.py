# -*- coding: utf-8 -*-

from configparser import ConfigParser
import argparse
import sys
import uuid

from db.engine import get_engine
from orquestador.models import StageConfig
from orquestador.config_loader import build_stage
from orquestador.paths import get_unc_path
from orquestador.logging_setup import setup_logging
from orquestador.pipeliine import Pipeline

def main():
    # Permite pasar argumentos desde la linea de comandos, como la ruta al archivo de configuracion
    parser = argparse.ArgumentParser(description="Pipeline para automatizar procesos en GCI")
    parser.add_argument('--config', help="Ruta del archivo de configuracion", default="proceso.ini")
    parser.add_argument('--batpath', type=str, help="Ruta completa del archivo .bat que ejecuto este script", default=None)
    args = parser.parse_args()
    
    # Cargar configuracion externa
    config = ConfigParser()
    config.read(args.config)
    
    # Configuracion general del log
    log_file = config.get("GENERAL", "log_file", fallback="proceso.log")
    logger = setup_logging(log_file)
    
    path_log_summ = config.get("GENERAL", "path_log_summ", fallback=None)
    process_name = config.get("GENERAL", "process_name", fallback=None)
    dev_mode = config.get("GENERAL", "dev_mode", fallback="false")
    
    # Establecer la conexion con la BBDD
    engine = get_engine()
    
    # Definir las etapas del pipeline basadas en el archivo de configuracion
    # Se puede tener en el archivo una seccion GENERAL y luego una seccion por cada etapa
    commands: list[StageConfig] = []
    for section in config.sections():
        if section != "GENERAL":
            commands.append(build_stage(config,section))
    
    # Obtener la ruta UNC del bat en caso llegue como ruta mapeada X:, Z:, etc
    bat_path = get_unc_path(args.batpath) if args.batpath else None
    ejecucion_id = str(uuid.uuid4())
    
    pipeline = Pipeline(
        commands,
        engine,
        process_name,
        log_file,
        path_log_summ,
        bat_path,
        dev_mode,
        ejecucion_id,
    )
    success = pipeline.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
    
        
    
    
