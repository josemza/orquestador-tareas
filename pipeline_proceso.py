# -*- coding: utf-8 -*-

import argparse
import configparser
from datetime import datetime
import getpass
from hashlib import sha256
from json import loads
import logging
from logging.handlers import RotatingFileHandler
import os
import socket
import subprocess
import sys
import uuid

import pandas as pd
from sqlalchemy import DateTime, Numeric
import win32wnet

from db.engine import get_engine
from send_to_cloud.http_conexion import post_to_flow

MAX_OUTPUT_CHARS = 60000
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_ENCODING = "utf-8"


def setup_logging(log_file, level= logging.INFO):
    """
    Configura el logging para que se registre log por consola y archivo

    Parameters
    ----------
    logFile : string
        nombre del archivo log.
    level : objeto, optional
        nivel de registro en el log. por defecto es logging.INFO.

    Returns
    -------
    logger

    """
    
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Handler para archivo
    file_handler = RotatingFileHandler(log_file)
    file_handler.setLevel(level)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # Formato de registro de log
    formatter = logging.Formatter(LOG_FORMAT, datefmt= LOG_DATE_FORMAT)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Agregar atributo con la ruta del log al logger
    setattr(logger, "log_file_path", file_handler.baseFilename)
    
    return logger

def get_unc_path(path):
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
    return sha256(bat_path_norm.encode("utf-8")).hexdigest()

class Pipeline:
    def __init__(self, commands,engine,process_name=None,path_log_summ=None,bat_path=None,dev_mode="False",ejecucion_id=None):
        """
        Clase para manejar las etapas del proceso

        Parameters
        ----------
        commands : list
            lista de diccionarios con la estructura:
                {
                    "name": <nombre_etapa>,
                    "command": <comando_a_ejecutar>,
                    "timeout": <tiempo_maximo_opcional>,
                    "result_key": <clave para almacenar la salida> (opcional)
                    "run_as_bat": <True/False> (opcional)
                }
        process_name: string
            Nombre del proceso (opcional)
        path_log_summ: string
            Ruta del archivo csv donde colocar el resumen del log (exitoso o error)
        bat_path: string
            Ruta del archivo bat que se ejecutara
        dev_mode: string
            Modo de desarrollo (opcional)
        ejecucion_id: string
            Id de ejecucion (opcional)

        Returns
        -------
        clase

        """
        
        self.commands = commands
        self.process_name = process_name
        self.path_log_summ = path_log_summ
        self.bat_path = bat_path
        self.start_time = datetime.now()
        self.end_time = datetime.now()
        self.engine = engine
        self.dev_mode = dev_mode
        self.proceso_id = fingerprint_from_norm_path(self.bat_path)
        self.ejecucion_id = ejecucion_id
        
        logging.info("="*50)
        logging.info(f"Nueva ejecucion {self.process_name}")
        logging.info("="*50)
    
    def insert_rows(self, datos, esquema="orquestador", tabla="log_procesos"):
        """
        Inserta los datos en la tabla log_procesos

        Parameters
        ----------
        datos : pandas.DataFrame
            DataFrame con los datos a insertar
        esquema : string
            Esquema donde se insertaran los datos (opcional)
        tabla : string
            Tabla donde se insertaran los datos (opcional)

        Returns
        -------
        None
        """
        datos["fecejec"] = pd.to_datetime(datos["fecejec"])
        datos["fecfin"] = pd.to_datetime(datos["fecfin"])

        try:
            logging.info(f"Insertando resumen del log en {esquema}.{tabla}...")
            datos.to_sql(tabla,
                         self.engine, 
                         if_exists="append", 
                         index=False,
                         schema=esquema,
                         dtype={
                             "fecejec": DateTime(),
                             "fecfin": DateTime(),
                             "duracion": Numeric(8,3)
                             }
                         )
        except:
            logging.warning(f"No se pudo insertar el resumen del log en {esquema}.{tabla}...")
    
    def log_summarized_dict(self, **kwargs):
        """
        Genera un diccionario con el resumen del log

        Parameters
        ----------
        **kwargs : dict
            Diccionario con los datos del log

        Returns
        -------
        dict
            Diccionario con el resumen del log
        """

        if "stage_name" and "output_error" in kwargs:
            result_message = f'Pipeline detenido por error en la etapa {kwargs["stage_name"]}. Detalle: \n{kwargs["output_error"][:MAX_OUTPUT_CHARS]}'
        elif "output_success" in kwargs:
            result_message = f'Pipeline finalizado exitosamente. Detalle: \n{kwargs["output_success"][:MAX_OUTPUT_CHARS]}'
        else:
            result_message = ""

        log_summ = {
                            "fecejec": [datetime.strftime(self.start_time, DEFAULT_DATE_FORMAT)],
                            "fecfin": [datetime.strftime(self.end_time, DEFAULT_DATE_FORMAT)],
                            "duracion":[(self.end_time - self.start_time).total_seconds() / 60],
                            "proceso": [self.process_name],
                            "resultado":[result_message],
                            "resumen": ["error" if "output_error" in kwargs else "exitoso"],
                            "rutalog": [getattr(logging.getLogger(), "log_file_path",None)],
                            "rutabat": [self.bat_path],
                            "usuario": [getpass.getuser().upper()],
                            "terminal":[socket.gethostname()],
                            "bat_sha256": [self.proceso_id],
                            "ejecucion_id": [self.ejecucion_id]
                        }
        return log_summ
    
    def send_log_summarized(self,esquema="orquestador",tabla="log_procesos"):
        """
        Envía el resumen del log por http post a un endpoint

        Parameters
        ----------
        esquema : string
            Esquema donde se consultan los datos (opcional)
        tabla : string
            Tabla donde se consultan los datos (opcional)

        Returns
        -------
        None
        """
        
        # - Consultar la vista de log para traer los datos de la tarea
        query = f"""
            SELECT FECEJEC, FECFIN, DURACION, PROCESO, RESUMEN, RESULTADO
            FROM {esquema}.{tabla}
            WHERE ejecucion_id = '{self.ejecucion_id}'
        """

        # - Ejecutar la consulta y guardarlo en un DataFrame
        if query:
            df = pd.read_sql(query,con=self.engine)
            logging.info(f"Consulta ok. Se obtuvieron {len(df)} filas.")
        
            # - Crear el diccionario
            log_dict = df.to_json(orient="records",date_format='iso')
            parsed = loads(log_dict)
            
            # - Enviar el resumen de log a power automate
            try:
                logging.info("Haciendo el http request a power automate...")
                # result = send_info_by_url(parsed[0])
                result = post_to_flow(parsed[0])
                logging.info(result)
            except Exception as e:
                logging.warning(f"Error al enviar correo: {e}")

    def run_stage(self, stage_name, command, timeout=None):
        """
        Ejecuta una etapa del pipeline y registra la salida y errores. 
        Si ocurre un error o se excede el tiempo de espera, retorna False

        Parameters
        ----------
        stage_name : string
            nombre de la etapa.
        command : string
            comando a ejecutar.
        timeout : int, optional
            tiempo de espera. por defecto es None.

        Returns
        -------
        Tupla (exito, salida)
            exito: True si el comando termino correctamente (codigo 0)
            salida: el contenido de stdout (strip() aplicado) o None en caso de error

        """
        
        logging.info("-"*50)
        logging.info(f"Iniciando etapa: {stage_name}")
        try:
            result =  subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                if result.stdout:
                    logging.info(f"{stage_name} OUTPUT:\n{result.stdout}")
                if result.stderr:
                    logging.warning(f"{stage_name} OUTPUT:\n{result.stderr}")
            
            if result.returncode != 0:
                logging.error(f"Error en {stage_name} (codigo: {result.returncode}). Deteniendo el pipeline")
                return False, result.stderr or result.stdout
            
            logging.info(f"{stage_name} completada exitosamente")
            return True, result.stdout.strip()
        except subprocess.TimeoutExpired as e:
            logging.error(f"Tiempo de espera excedido en {stage_name}: {e}")
            return False, None
        except Exception as e:
            logging.error(f"Excepcion en {stage_name}: {e}")
            return False, e
    
    def run(self):
        """
        Ejecuta todas la etapas en secuencia. Si alguna falla, detiene el pipeline.
        Permite capturar parametros producidos en una etapa y usarlos en comandos posteriores,
        basandose en la opcion 'result_key' definida en el archivo de configuracion.
        Permite ejecutar BAT en paralelo sin esperar resultado

        Returns
        -------
        Booleano

        """
        
        captured_params = {}
        output_successfully = {}
        
        #Agregar la fecha de ejecucion automaticamente
        now = datetime.now()
        captured_params["anio"] = now.strftime("%Y")
        captured_params["mes"] = now.strftime("%m")
        captured_params["dia"] = now.strftime("%d")        
        
        for stage in self.commands:
            stage_name = stage["name"]
            condicion = stage["condicion"]
            
            # Si se define la condición, se evalúa antes de ejecutar la etapa
            if condicion:
                try:
                    # Se sustituyen las variables capturadas en la condición
                    condicion_formateada = condicion.format(**captured_params)
                    # Se evalúa la condición. Debe retornar un valor booleano.
                    if not eval(condicion_formateada):
                        logging.info(f"Se omite la etapa '{stage_name}' por la condición: {condicion_formateada}")
                        continue  # Se salta esta etapa
                except KeyError as e:
                    logging.error(f"Falta el parámetro {e} para evaluar la condición en la etapa '{stage_name}'.")
                    return False
                except Exception as e:
                    logging.error(f"Error al evaluar la condición en la etapa '{stage_name}': {e}")
                    return False
            
            command = stage["command"]
            timeout = stage.get("timeout", None)
            run_as_bat = stage.get("run_as_bat","False").lower() == "true"
            
            if run_as_bat: # Verifica si en la estapa se ha configurado la ejecucion de un BAT
                logging.info("-"*50)
                logging.info(f"Iniciando etapa (BAT): {stage_name} ejecutando: {command}")
                try:
                    # Ejecuta el archivo BAT. Se lanza sin esperar que termine
                    subprocess.Popen(f'start "" /min "{command}"',shell=True)
                    #subprocess.Popen(command, shell=True)
                    success = True
                    output = "Bat iniciado"
                except Exception as e:
                    logging.error(f"Error ejecutando BAT en {stage_name}: {e}")
                    success = False
                    output = str(e)
            else:
                # Si existen parametros capturados, se sustituyen en el comando (usando formato)
                if captured_params:
                    try:
                        command = command.format(**captured_params)
                    except KeyError as e:
                        logging.error(f"Falta el parametro {e} para el comando en la etapa '{stage_name}'.")
                        return False
                
                success, output = self.run_stage(stage_name, command, timeout)
                if not success:
                    logging.info("-"*50)
                    logging.error(f"##> RESULTADO: Pipeline detenido por error en la etapa {stage_name}.")
                    
                    self.end_time = datetime.now() # actualizar hora de fin
                    output_error = ''
                    if output:
                        output_error = output[:MAX_OUTPUT_CHARS]
                    log_summ = self.log_summarized_dict(stage_name=stage_name, output_error=output_error)
                    summ_pandas = pd.DataFrame(log_summ)
                    
                    # Si esta en modo desarrollo no cargar el resumen a la bbdd ni desencadenar el power automate
                    if not (self.dev_mode.lower() == "true"):
                        # Insertar resumen en la bd
                        self.insert_rows(summ_pandas)
                    
                        # Enviar el log a power automate
                        self.send_log_summarized()
                        
                    return False
                
                # si en la configuracion se definio 'result_key', se guarda la salida
                result_key = stage.get("result_key", None)
                if result_key:
                    captured_params[result_key] = output
                    logging.info(f"parametro capturado: {result_key} = {output}")
                
                # si en la configuracion se definio show_output = 'true', se guarda la salida
                show_output = stage.get("show_output","False").lower() == "true"
                if show_output:
                    output_successfully[stage_name] = output
        
        logging.info("-"*50)
        logging.info("##> RESULTADO: Pipeline completado exitosamente")

        # Concatenar todos los outputs que el usuario solicito en la configuracion
        line = "-"*50
        output_successfully_text = ''
        for key, value in output_successfully.items():
            output_successfully_text += f"[Salida de la etapa: {key}]\n{line}\n{value}\n\n"

        self.end_time = datetime.now() # actualizar hora de fin
        log_summ = self.log_summarized_dict(output_success=output_successfully_text)
        summ_pandas = pd.DataFrame(log_summ)

        # Si esta en modo desarrollo no cargar el resumen a la bd ni desencadenar el power automate
        if not (self.dev_mode.lower() == "true"):
            # Insertar resumen en la bd
            self.insert_rows(summ_pandas)
        
            # Enviar el log a power automate
            self.send_log_summarized()
            
        return True

def main():
    # Permite pasar argumentos desde la linea de comandos, como la ruta al archivo de configuracion
    parser = argparse.ArgumentParser(description="Pipeline para automatizar procesos en GCI")
    parser.add_argument('--config', help="Ruta del archivo de configuracion", default="proceso.ini")
    parser.add_argument('--batpath', type=str, help="Ruta completa del archivo .bat que ejecuto este script", default=None)
    args = parser.parse_args()
    
    # Cargar configuracion externa
    config = configparser.ConfigParser()
    config.read(args.config)
    
    # Configuracion general del log
    log_file = config.get("GENERAL", "log_file", fallback="proceso.log")
    setup_logging(log_file)
    
    path_log_summ = config.get("GENERAL", "path_log_summ", fallback=None)
    process_name = config.get("GENERAL", "process_name", fallback=None)
    dev_mode = config.get("GENERAL", "dev_mode", fallback="false")
    
    # Establecer la conexion con la BBDD
    engine = get_engine()
    
    # Definir las etapas del pipeline basadas en el archivo de configuracion
    # Se puede tener en el archivo una seccion GENERAL y luego una seccion por cada etapa
    commands = []
    for section in config.sections():
        if section != "GENERAL":
            stage_name = section
            command = config.get(section, "command")
            timeout = config.getint(section, "timeout", fallback=None)
            result_key = config.get(section, "result_key", fallback=None)
            condicion = config.get(section, "condicion", fallback=None)
            run_as_bat = config.get(section, "run_as_bat", fallback="false")
            show_output = config.get(section, "show_output", fallback="false")
            commands.append({
                "name": stage_name, 
                "command": command, 
                "timeout": timeout,
                "result_key": result_key,
                "condicion": condicion,
                "run_as_bat": run_as_bat,
                "show_output": show_output
                })
    
    # Obtener la ruta UNC del bat en caso llegue como ruta mapeada X:, Z:, etc
    bat_path = get_unc_path(args.batpath)
    ejecucion_id = str(uuid.uuid4())
    
    pipeline = Pipeline(commands,engine,process_name,path_log_summ,bat_path,dev_mode, ejecucion_id)
    success = pipeline.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    