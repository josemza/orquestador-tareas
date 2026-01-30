from datetime import datetime
import getpass
from json import loads
import logging
import socket
import subprocess
import time
from typing import Optional, Any

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.types import DateTime, Numeric

from send_to_cloud.http_conexion import post_to_flow
from orquestador.models import StageConfig, StageResult
from orquestador.paths import fingerprint_from_norm_path
from orquestador.constants import MAX_OUTPUT_CHARS, DEFAULT_DATE_FORMAT


class Pipeline:
    def __init__(self, commands: list[StageConfig],engine: Engine,process_name: str = None,log_file: str = None,path_log_summ: str = None,bat_path: str = None,dev_mode: str = "False",ejecucion_id: str = None) -> None:
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
        engine: sqlalchemy.engine.base.Engine
            engine de sqlalchemy con la conexión a la base de datos
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
        
        self.commands: list[StageConfig] = commands
        self.process_name = process_name
        self.path_log_summ = path_log_summ
        self.log_file = log_file
        self.bat_path = bat_path
        self.start_time = datetime.now()
        self.end_time = datetime.now()
        self.engine = engine
        self.dev_mode = dev_mode
        self.proceso_id = fingerprint_from_norm_path(self.bat_path) if self.bat_path else None
        self.ejecucion_id = ejecucion_id
        
        logging.info("="*50)
        logging.info(f"Nueva ejecucion {self.process_name}")
        logging.info("="*50)
    
    def _should_skip_stage(self, stage: StageConfig, captured_params: dict[str,Any]) -> bool:
        # Se sustituyen las variables capturadas en la condición
        condicion_formateada = stage.condicion.format(**captured_params)
        # Se evalúa la condición. Debe retornar un valor booleano.
        if not eval(condicion_formateada, {"__builtins__": {}}, captured_params):
            return True

        return False
    
    def _record_stage_output(self, stage: StageConfig, result: StageResult, output_successfully: dict[str,str], captured_params: dict[str,Any]) -> None:
        if stage.result_key:
            captured_params[stage.result_key] = result.stdout # output
            logging.info(f"parametro capturado: {stage.result_key} = {result.stdout}")
            if stage.run_as_bat and (result.error or "").strip():
                logging.warning(result.error)
        
        if stage.show_output:
            output_successfully[stage.name] = result.stdout # output

    def insert_rows(self, datos: pd.DataFrame, esquema: str = "orquestador", tabla: str = "log_procesos") -> None:
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
        except Exception:
            logging.warning(f"No se pudo insertar el resumen del log en {esquema}.{tabla}...")
    
    def log_summarized_dict(self, **kwargs) -> dict[str, list[Any]]:
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

        if "stage_name" in kwargs and "output_error" in kwargs:
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
                            "rutalog": [self.log_file],
                            "rutabat": [self.bat_path],
                            "usuario": [getpass.getuser().upper()],
                            "terminal":[socket.gethostname()],
                            "bat_sha256": [self.proceso_id],
                            "ejecucion_id": [self.ejecucion_id]
                        }
        return log_summ
    
    def send_log_summarized(self,esquema: str ="orquestador",tabla: str ="log_procesos") -> None:
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

    def run_stage(self, stage_name: str, command: str, timeout: Optional[int] = None) -> StageResult:
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
        StageResult
            ok: True si el comando termino correctamente (codigo 0)
            returncode: el codigo de retorno del comando
            stdout: el contenido de stdout (strip() aplicado) o None en caso de error
            stderr: el contenido de stderr (strip() aplicado) o None en caso de error
            error: el contenido de la excepcion o None en caso de error
            duration_s: el tiempo de ejecucion en segundos

        """
        
        logging.info("-"*50)
        logging.info(f"Iniciando etapa: {stage_name}")
        try:
            t0 = time.perf_counter()
            result =  subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
            elapsed = time.perf_counter() - t0
            if result.returncode == 0:
                if result.stdout:
                    logging.info(f"{stage_name} OUTPUT:\n{result.stdout}")
                if result.stderr:
                    logging.warning(f"{stage_name} STDERR:\n{result.stderr}")
            
            if result.returncode != 0:
                logging.error(f"Error en {stage_name} (codigo: {result.returncode}). Deteniendo el pipeline")
                return StageResult(
                    ok=False,
                    returncode=result.returncode,
                    stdout=(result.stdout or "").strip(),
                    stderr=(result.stderr or "").strip(),
                    duration_s=elapsed/60,
                    fatal=True
                )
            
            logging.info(f"{stage_name} completada exitosamente")
            return StageResult(
                ok=True,
                returncode=0,
                stdout=(result.stdout or "").strip(),
                stderr=(result.stderr or "").strip(),
                duration_s=elapsed/60,
                fatal=False
            )

        except subprocess.TimeoutExpired as e:
            elapsed = time.perf_counter() - t0
            logging.error(f"Tiempo de espera excedido en {stage_name}: {e}")
            return StageResult(
                ok=False,
                returncode=1,
                stdout="",
                stderr="",
                error=str(e).strip(),
                duration_s=elapsed/60,
                fatal=True
            )
        except Exception as e:
            elapsed = time.perf_counter() - t0
            logging.error(f"Excepcion en {stage_name}: {e}")
            return StageResult(
                ok=False,
                returncode=1,
                stdout="",
                stderr="",
                error=str(e).strip(),
                duration_s=elapsed/60,
                fatal=True
            )
    
    def run(self) -> bool:
        """
        Ejecuta todas la etapas en secuencia. Si alguna falla, detiene el pipeline.
        Permite capturar parametros producidos en una etapa y usarlos en comandos posteriores,
        basandose en la opcion 'result_key' definida en el archivo de configuracion.
        Permite ejecutar BAT en paralelo sin esperar resultado

        Returns
        -------
        Booleano

        """
        
        captured_params: dict[str, Any] = {}
        output_successfully: dict[str, str] = {}
        
        #Agregar la fecha de ejecucion automaticamente
        now = datetime.now()
        captured_params["anio"] = now.strftime("%Y")
        captured_params["mes"] = now.strftime("%m")
        captured_params["dia"] = now.strftime("%d")        
        
        for stage in self.commands:           
            # Si se define la condición, se evalúa antes de ejecutar la etapa
            if stage.condicion:
                try:
                    skip_stage = self._should_skip_stage(stage,captured_params)
                    if skip_stage:
                        logging.info(f"Se omite la etapa '{stage.name}' por la condición: {stage.condicion.format(**captured_params)}")
                        continue  # Se salta esta etapa
                except KeyError as e:
                    logging.error(f"Falta el parámetro {e} para evaluar la condición en la etapa '{stage.name}'.")
                    return False
                except Exception as e:
                    logging.error(f"Error al evaluar la condición en la etapa '{stage.name}': {e}")
                    return False
            
            if stage.run_as_bat: # Verifica si en la estapa se ha configurado la ejecucion de un BAT
                logging.info("-"*50)
                logging.info(f"Iniciando etapa (BAT): {stage.name} ejecutando: {stage.command}")
                try:
                    # Ejecuta el archivo BAT. Se lanza sin esperar que termine
                    subprocess.Popen(f'start "" /min "{stage.command}"',shell=True)
                    result = StageResult(
                        ok=True,
                        returncode=0,
                        stdout="iniciado",
                        stderr="",
                        duration_s=0,
                        fatal=False
                    )

                    self._record_stage_output(stage,result,output_successfully,captured_params)

                except Exception as e:
                    logging.error(f"Error ejecutando BAT en {stage.name}: {e}")
                    result = StageResult(
                        ok=False,
                        returncode=1,
                        stdout="no_iniciado",
                        stderr="",
                        error=f"Error al iniciar BAT paralelo en {stage.name}: {e}",
                        duration_s=0,
                        fatal=False
                    )

                    self._record_stage_output(stage,result,output_successfully,captured_params)
            else:
                # Si existen parametros capturados, se sustituyen en el comando (usando formato)
                command_params = stage.command
                if captured_params:
                    try:
                        command_params = stage.command.format(**captured_params)
                    except KeyError as e:
                        logging.error(f"Falta el parametro {e} para el comando en la etapa '{stage.name}'.")
                        return False
                
                result = self.run_stage(stage.name, command_params or stage.command, stage.timeout)
                if not result.ok and result.fatal:
                    logging.info("-"*50)
                    logging.error(f"##> RESULTADO: Pipeline detenido por error en la etapa {stage.name}.")
                    
                    self.end_time = datetime.now() # actualizar hora de fin
                    output_error = result.stderr or result.stdout or result.error or ""
                    log_summ = self.log_summarized_dict(stage_name=stage.name, output_error=output_error[:MAX_OUTPUT_CHARS])
                    summ_pandas = pd.DataFrame(log_summ)
                    
                    # Si esta en modo desarrollo no cargar el resumen a la bbdd ni desencadenar el power automate
                    if not (self.dev_mode.lower() == "true"):
                        # Insertar resumen en la bd
                        self.insert_rows(summ_pandas)
                    
                        # Enviar el log a power automate
                        self.send_log_summarized()
                        
                    return False
                
                self._record_stage_output(stage,result,output_successfully,captured_params)
        
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
