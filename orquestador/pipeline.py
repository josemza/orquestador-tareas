from datetime import datetime
import getpass
import hashlib
from json import loads, dumps
from html import escape
import logging
import re
import socket
import subprocess
import time
from typing import Any, Optional
import pandas as pd
from sqlalchemy import DateTime, Numeric, text, LargeBinary
from sqlalchemy.engine import Engine

from orquestador.constants import DEFAULT_DATE_FORMAT, MAX_OUTPUT_CHARS, MAX_BYTES
from orquestador.models import StageConfig, StageResult, GeneralConfig
from orquestador.paths import fingerprint_from_norm_path
from send_to_cloud.http_conexion import post_to_flow
from mapeo.identificar_etapa import mapeo_etapa
from mapeo.cargar_mapeo_bbdd import insert_rows


class Pipeline:
    def __init__(self, commands: list[StageConfig],engine: Engine,general_config: GeneralConfig,bat_path: str = None,ejecucion_id: str = None,initial_params: dict = None) -> None:
        """
        Clase para manejar las etapas del proceso

        Parameters
        ----------
        commands : list
            lista de diccionarios con la estructura:
                {
                    "name": <nombre_etapa>,
                    "type": <command|query> (opcional, por defecto command)
                    "command": <comando_a_ejecutar>,
                    "timeout": <tiempo_maximo_opcional>,
                    "result_key": <clave para almacenar la salida> (opcional)
                    "run_as_bat": <True/False> (opcional)
                    "allow_error": <True/False> (opcional)
                    "query_sql": <consulta_select> (Requerido para query)
                    "query_title": <titulo_html_consulta> (opcional)
                }
        engine: sqlalchemy.engine.base.Engine
            engine de sqlalchemy con la conexión a la base de datos
        process_name: string
            Nombre del proceso (opcional)
        path_log_summ: string
            Ruta del archivo csv donde colocar el resumen del log (exitoso o error)
        bat_path: string
            Ruta del archivo bat que se ejecutara
        dev_mode: bool
            Modo de desarrollo (opcional)
        ejecucion_id: string
            Id de ejecucion (opcional)

        Returns
        -------
        clase

        """
        
        self.commands: list[StageConfig] = commands
        self.process_name: str = general_config.process_name
        self.path_log_summ: str = general_config.path_log_summ
        self.log_file: str = general_config.log_file
        self.bat_path: str = bat_path
        self.start_time: datetime = None
        self.end_time: datetime = None
        self.engine: Engine = engine
        self.dev_mode: bool = general_config.dev_mode
        self.proceso_id: str = fingerprint_from_norm_path(self.bat_path) if self.bat_path else None
        self.ejecucion_id: str = ejecucion_id       
        self.initial_params = initial_params or {}
        self.query_sections: list[dict[str, Any]] = []
    
    def _welcome_message(self) -> None:
        logging.info("="*50)
        logging.info(f"Nueva ejecucion {self.process_name}")
        logging.info(f"Id ejecucion: {self.ejecucion_id}")
        logging.info("="*50)

    @staticmethod
    def _clean_sql(query: str) -> str:
        return (query or "").strip().rstrip(";").strip()
    
    def _is_safe_select_query(self, query: str) -> bool:
        clean = self._clean_sql(query)
        if not clean:
            return False
        
        # Evita multiples sentencias y operaciones DML/DDL
        dangerous_tokens = (
            r"\binsert\b", r"\bupdate\b", r"\bdelete\b", r"\bmerge\b",
            r"\bdrop\b", r"\balter\b", r"\btruncate\b", r"\bcreate\b",
            r"\bgrant\b", r"\brevoke\b", r"\bexecute\b", r"\bcall\b"
        )
        if ";" in clean:
            return False
        if not re.match(r"^\s*select\b", clean, flags=re.IGNORECASE):
            return False
        return not any(re.search(token, clean, flags=re.IGNORECASE) for token in dangerous_tokens)

    def _build_limited_query(self, query: str, limit: int = 20) -> str:
        clean = self._clean_sql(query)
        dialect_name = (self.engine.dialect.name or "").lower()

        # Se envuelve para forzar limite y evitar dependencia de la query original
        if dialect_name in {"oracle"}:
            return f"SELECT * FROM ({clean}) q FETCH FIRST {limit} ROWS ONLY"
        return f"SELECT * FROM ({clean}) q LIMIT {limit}"
    
    @staticmethod
    def _render_query_result_html(df: pd.DataFrame, title: str, max_rows: int = 20) -> str:
        styles = (
            "border-collapse:collapse;width:100%;font-family:Segoe UI,Arial,sans-serif;"
            "font-size:12px;color:#222;"
        )
        th_style = "border:1px solid #d9d9d9;padding:8px;background:#f3f6fa;text-align:left;"
        td_style = "border:1px solid #d9d9d9;padding:8px;text-align:left;vertical-align:top;"

        head_cells = "".join(f"<th style='{th_style}'>{escape(str(col))}</th>" for col in df.columns)
        rows_html = []
        for _, row in df.iterrows():
            row_cells = "".join(
                f"<td style='{td_style}'>{escape('' if pd.isna(v) else str(v))}</td>"
                for v in row.values
            )
            rows_html.append(f"<tr>{row_cells}</tr>")

        body_rows = "".join(rows_html) if rows_html else (
            f"<tr><td colspan='{max(1, len(df.columns))}' style='{td_style}'>Sin registros</td></tr>"
        )

        return (
            f"<h3 style='font-family:Segoe UI,Arial,sans-serif;color:#1f2937;'>{escape(title)}</h3>"
            f"<p style='font-family:Segoe UI,Arial,sans-serif;font-size:12px;color:#4b5563;'>"
            f"Mostrando maximo {max_rows} registros.</p>"
            f"<table style='{styles}'>"
            f"<thead><tr>{head_cells}</tr></thead>"
            f"<tbody>{body_rows}</tbody>"
            f"</table>"
        )

    def _build_query_section(self, stage: StageConfig, captured_params: dict[str, Any]) -> dict[str, Any]:
        max_rows = stage.max_rows if stage.max_rows else 20
        if max_rows < 1:
            max_rows = 1
        if max_rows > 20:
            max_rows = 20

        if not stage.query_sql:
            return {
                "stage_name": stage.name,
                "status": "error",
                "message": "No se configuro query_sql",
                "html": "<p style='font-family:Segoe UI,Arial,sans-serif;'>Consulta no disponible: falta query_sql.</p>",
                "row_count": 0,
                "max_rows": max_rows,
                "truncated": False,
                "query_hash": None,
            }

        try:
            query_sql = stage.query_sql.format(**captured_params)
            query_title = (stage.query_title or stage.name).format(**captured_params)
        except KeyError as e:
            return {
                "stage_name": stage.name,
                "status": "error",
                "message": f"Falta parametro para construir query: {e}",
                "html": "<p style='font-family:Segoe UI,Arial,sans-serif;'>Consulta no disponible por parametros faltantes.</p>",
                "row_count": 0,
                "max_rows": max_rows,
                "truncated": False,
                "query_hash": None,
            }

        if not self._is_safe_select_query(query_sql):
            return {
                "stage_name": stage.name,
                "status": "error",
                "message": "La consulta no cumple las reglas de seguridad (solo SELECT simple).",
                "html": "<p style='font-family:Segoe UI,Arial,sans-serif;'>Consulta no permitida por reglas de seguridad.</p>",
                "row_count": 0,
                "max_rows": max_rows,
                "truncated": False,
                "query_hash": hashlib.sha256(self._clean_sql(query_sql).encode("utf-8")).hexdigest(),
            }

        base_query = self._clean_sql(query_sql)
        query_hash = hashlib.sha256(base_query.encode("utf-8")).hexdigest()
        limited_query = self._build_limited_query(base_query, max_rows)

        try:
            df = pd.read_sql(text(limited_query), con=self.engine)
            html_table = self._render_query_result_html(df, query_title, max_rows=max_rows)
            return {
                "stage_name": stage.name,
                "status": "ok",
                "message": None,
                "html": html_table,
                "row_count": int(len(df)),
                "max_rows": max_rows,
                "truncated": bool(len(df) >= max_rows),
                "query_hash": query_hash,
            }
        except Exception as e:
            logging.warning(f"Error ejecutando consulta adicional: {e}")
            return {
                "stage_name": stage.name,
                "status": "error",
                "message": str(e),
                "html": "<p style='font-family:Segoe UI,Arial,sans-serif;'>No se pudo obtener la consulta solicitada.</p>",
                "row_count": 0,
                "max_rows": max_rows,
                "truncated": False,
                "query_hash": query_hash,
            }

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

    def _make_dataframe_log_stage(self) -> pd.DataFrame:
        columnas = ["ejecucion_id","stagename","ok","returncode","stdout","stderr","duration_s","fatal","stage_category"]
        
        return pd.DataFrame(columns=columnas)
    
    def insert_rows(self, datos: pd.DataFrame, esquema: str = "usbds01", tabla: str = "log_procesos", dtypearg: dict[str,Any] = None) -> None:
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

        try:
            logging.info(f"Insertando resumen del log en {esquema}.{tabla}...")
            datos.to_sql(tabla,
                         self.engine, 
                         if_exists="append", 
                         index=False,
                         schema=esquema,
                         dtype=dtypearg
                         )
        except Exception as e:
            logging.warning(f"No se pudo insertar el resumen del log en {esquema}.{tabla}...{e}")
    
    def log_summarized_pandas(self, **kwargs) -> tuple[pd.DataFrame,dict[str,Any]]:
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
        
        log_summ_pd = pd.DataFrame(log_summ)
        log_summ_pd["fecejec"] = pd.to_datetime(log_summ_pd["fecejec"])
        log_summ_pd["fecfin"] = pd.to_datetime(log_summ_pd["fecfin"])

        dtype= {
                    "fecejec": DateTime(),
                    "fecfin": DateTime(),
                    "duracion": Numeric(8,3)
                }

        return log_summ_pd, dtype
    
    def log_stage_dict(self, stage_result: StageResult, stage_name: str, stage_category: str) -> dict[str,list[Any]]:
        log_stage = {
            "ejecucion_id": self.ejecucion_id,
            "stagename": stage_name,
            "ok": stage_result.ok,
            "returncode": stage_result.returncode,
            "stdout": stage_result.stdout,
            "stderr": stage_result.stderr,
            "duration_s": stage_result.duration_s,
            "fatal": stage_result.fatal,
            "stage_category": stage_category
        }

        return log_stage
    
    def append_log_stages(self, log_stage: dict[str,Any], datacol: pd.DataFrame) -> None:
        datos_mapeados = {k: (int(v) if isinstance(v, bool) else v) for k, v in log_stage.items()}
        datacol.loc[len(datacol)] = datos_mapeados

    def send_log_summarized(self,esquema: str ="usods01",tabla: str ="log_procesos") -> None:
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
            SELECT FECEJEC, FECFIN, DURACION, PROCESO, TASKNAME, RESUMEN, RESULTADO, CARPETAPROCESO, RUTALOG, RUTABAT, EJECUTADO_POR, 
            EJECUTADO_EN, PROGRAMADO_EN, NEXTRUNTIME, PROGRAMACIONTEXT
            FROM {esquema}.{tabla}
            WHERE ejecucion_id = '{self.ejecucion_id}'
        """

        # - Ejecutar la consulta y guardarlo en un DataFrame
        if query:
            df = pd.read_sql(query,con=self.engine)
            logging.info(f"Consulta ok. Se obtuvieron {len(df)} filas.")
        
            # - Crear el bloque log
            log_dict = df.to_json(orient="records",date_format='iso')
            parsed = loads(log_dict)
            
            log_summary = parsed[0] if parsed else {}

            unified_payload = {
                "ejecucion_id": self.ejecucion_id,
                "process_name": self.process_name,
                "requested_by": getpass.getuser().upper(),
                "requested_at": datetime.now().strftime(DEFAULT_DATE_FORMAT),
                "log_summary": log_summary,
                "query_sections": self.query_sections,
            }

            logging.info(unified_payload)

            # - Enviar el resumen de log y consultas a power automate
            try:
                logging.info("Haciendo el http request a power automate...")
                # result = send_info_by_url(parsed[0])
                result = post_to_flow(unified_payload)                
                logging.info(result)

                if result[0].status_code < 200 or result[0].status_code >= 300:
                    contingencia_envios = {
                        "ejecucion_id": self.ejecucion_id,
                        "estado": "no enviado",
                        "query_sections": self.query_sections,
                    }
                    contingencia_envios_df = pd.DataFrame.from_dict([contingencia_envios])
                    contingencia_envios_df["query_sections"] = contingencia_envios_df["query_sections"].apply(lambda x: dumps(x).encode("utf-8") if x is not None else None)
                    # self.insert_rows(contingencia_envios_df,"orquestador","log_contingencia_envios",{"query_sections": LargeBinary})
            except Exception as e:
                contingencia_envios = {
                    "ejecucion_id": self.ejecucion_id,
                    "estado": "no enviado",
                    "query_sections": self.query_sections,
                }
                contingencia_envios_df = pd.DataFrame.from_dict([contingencia_envios])
                contingencia_envios_df["query_sections"] = contingencia_envios_df["query_sections"].apply(lambda x: dumps(x).encode("utf-8") if x is not None else None)
                # self.insert_rows(contingencia_envios_df,"orquestador","log_contingencia_envios",{"query_sections": LargeBinary})
                logging.warning(f"Error al enviar correo: {e}")

    def run_stage(self, stage_name: str, command: str, timeout: Optional[int] = None, allow_error: Optional[bool] = True) -> StageResult:
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
                if allow_error:
                    logging.warning(f"Error en {stage_name}. **No se detiene el pipeline por configuracion allow_error = {allow_error}")
                else:
                    logging.error(f"Error en {stage_name} (codigo: {result.returncode}). Deteniendo el pipeline. Detalle:\n{result.stderr or result.stdout}")
                return StageResult(
                    ok=False,
                    returncode=result.returncode,
                    stdout=(result.stdout or "").strip(),
                    stderr=(result.stderr or "").strip(),
                    duration_s=elapsed,
                    fatal= True if not allow_error else False
                )
            
            logging.info(f"{stage_name} completada exitosamente")
            return StageResult(
                ok=True,
                returncode=0,
                stdout=(result.stdout or "").strip(),
                stderr=(result.stderr or "").strip(),
                duration_s=elapsed,
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
                duration_s=elapsed,
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
                duration_s=elapsed,
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
        self.start_time = datetime.now()
        self._welcome_message()

        captured_params: dict[str, Any] = self.initial_params.copy()
        output_successfully: dict[str, str] = {}
        stage_log_df: pd.DataFrame = self._make_dataframe_log_stage()
        
        #Agregar la fecha de ejecucion automaticamente
        now = datetime.now()
        captured_params["anio"] = now.strftime("%Y")
        captured_params["mes"] = now.strftime("%m")
        captured_params["dia"] = now.strftime("%d")   

        logging.info(f"Parametros iniciales disponibles: {captured_params.keys()}")     
        
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
            
            if stage.stage_type == "query":
                logging.info("-"*50)
                logging.info(f"Iniciando etapa (QUERY): {stage.name}")
                query_section = self._build_query_section(stage, captured_params)
                self.query_sections.append(query_section)
                if stage.result_key:
                    captured_params[stage.result_key] = str(query_section.get("row_count", 0))
                    logging.info(f"parametro capturado: {stage.result_key} = {captured_params[stage.result_key]}")
                if query_section.get("status") == "error":
                    logging.warning(f"Etapa QUERY con error en '{stage.name}': {query_section.get('message')}")
                else:
                    logging.info(
                        f"Etapa Query '{stage.name}' OK. Filas: {query_section.get('row_count', 0)} "
                        f"(max {query_section.get('max_rows', 20)})"
                    )
                continue

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
                
                result = self.run_stage(stage.name, command_params or stage.command, stage.timeout, stage.allow_error)
                if not result.ok and result.fatal:
                    logging.info("-"*50)
                    logging.error(f"##> RESULTADO: Pipeline detenido por error en la etapa {stage.name}.")

                    self.end_time = datetime.now() # actualizar hora de fin

                    # Si esta en modo desarrollo no cargar el resumen a la bbdd ni desencadenar el power automate
                    if not self.dev_mode:
                        output_error = result.stderr or result.stdout or result.error or ""

                        b = output_error.encode("utf-8", errors="replace")
                        if len(b) > MAX_BYTES:
                            detalle_db = b[:MAX_BYTES].decode("utf-8", errors="ignore") + " ...[TRUNCADO]"
                        else:
                            detalle_db = output_error

                        log_summ,dtypes = self.log_summarized_pandas(stage_name=stage.name, output_error=detalle_db)
                        self.insert_rows(datos=log_summ,esquema="orquestador",tabla="log_procesos",dtypearg=dtypes) # Insertar resumen log en la bd

                        # log_stage = self.log_stage_dict(stage_result=result,stage_name=stage.name,stage_category="")
                        # self.append_log_stages(log_stage=log_stage,datacol=stage_log_df)
                        self.insert_rows(datos=stage_log_df,esquema="orquestador",tabla="log_stage_proceso",dtypearg={"duration_s": Numeric(8,3)})
                    
                        # Enviar el log a power automate
                        self.send_log_summarized(esquema="orquestador", tabla="log_procesos_view")
                        
                    return False                

                self._record_stage_output(stage,result,output_successfully,captured_params)

                if not self.dev_mode:
                    # mapear etapa
                    tipo_etapa, info_etapa = mapeo_etapa(stage.command)
                    if info_etapa:
                        info_etapa_df = pd.DataFrame(info_etapa)
                        insert_rows(self.engine,info_etapa_df,self.proceso_id,tipo_etapa)
                    
                    # Guardar log del stage en la BD
                    log_stage = self.log_stage_dict(stage_result=result,stage_name=stage.name,stage_category=tipo_etapa)                        
                    self.append_log_stages(log_stage=log_stage,datacol=stage_log_df)
        
        logging.info("-"*50)
        logging.info("##> RESULTADO: Pipeline completado exitosamente")

        # Concatenar todos los outputs que el usuario solicito en la configuracion
        line = "-"*50
        output_successfully_text = ''
        for key, value in output_successfully.items():
            output_successfully_text += f"[Salida de la etapa: {key}]\n{line}\n{value}\n\n"

        self.end_time = datetime.now() # actualizar hora de fin

        # Si esta en modo desarrollo no cargar el resumen a la bd ni desencadenar el power automate
        if not self.dev_mode:
            log_summ,dtypes = self.log_summarized_pandas(output_success=output_successfully_text)
            self.insert_rows(datos=log_summ,esquema="orquestador",tabla="log_procesos",dtypearg=dtypes) # Insertar resumen log en la bd
            self.insert_rows(datos=stage_log_df,esquema="orquestador",tabla="log_stage_proceso",dtypearg={"duration_s": Numeric(8,3)}) # Insertar log de stages en la bd
        
            # Enviar el log a power automate
            self.send_log_summarized(esquema="orquestador", tabla="log_procesos_view")
            
        return True
