# Orquestador de Tareas Automatizadas

## Descripcion
Este proyecto ejecuta procesos automatizados por etapas, registra el resultado en base de datos y envia un reporte a Power Automate mediante HTTP.

El pipeline soporta:
- Etapas de comando (`type=command`) para ejecutar scripts/comandos del sistema.
- Etapas de consulta (`type=query`) para consultar base de datos y enviar tablas HTML en correo.
- Condiciones entre etapas usando parametros capturados (`result_key`).
- Ejecucion paralela de `.bat` (`run_as_bat=true`) sin esperar su finalizacion.

## Arquitectura Rapida
### Flujo general
1. `pipeline_proceso.py` lee el archivo `.ini`.
2. `orquestador/config_loader.py` convierte cada seccion del `.ini` en `StageConfig`.
3. `orquestador/pipeliine.py` ejecuta etapas en orden.
4. Se guarda resumen de ejecucion en `orquestador.log_procesos` (si `dev_mode=false`).
5. Se arma un payload unificado con:
- `log_summary`
- `query_sections` (0..N consultas con HTML)
6. `send_to_cloud/http_conexion.py` envia el payload a Power Automate con `post_to_flow`.

### Componentes principales
- `pipeline_proceso.py`: punto de entrada.
- `orquestador/models.py`: modelos `StageConfig` y `StageResult`.
- `orquestador/config_loader.py`: parser de configuracion por etapa.
- `orquestador/pipeliine.py`: motor del pipeline (comandos, queries, logs, envio).
- `db/engine.py`: conexion SQLAlchemy.
- `send_to_cloud/http_conexion.py`: cliente HTTP para Power Automate.

## Prerrequisitos
- Python 3.10+ (recomendado usar `.venv`).
- Dependencias instaladas del proyecto.
- Conexion a BD configurada por:
- modulo corporativo `conexion.conexion.get_engine()`, o
- variable `DB_URL` en `.env`.
- `URL_FLOW` en `.env` para el trigger HTTP de Power Automate.

## Configuracion de `.env` (minima)
```env
URL_FLOW=https://prod-xx.westus.logic.azure.com:443/workflows/...
PATH_LOG=logs/power_auto_http_req/pa_sender.jsonl
DB_URL=postgresql+psycopg2://usuario:password@host:5432/base
```

## Manual del archivo `.ini`
El `.ini` tiene una seccion `[GENERAL]` y luego una seccion por etapa.

### 1. Seccion `[GENERAL]`
Campos mas usados:
- `process_name`: nombre del proceso.
- `log_file`: ruta del log local.
- `dev_mode`: `true|false`.
- `path_log_summ`: opcional.

Ejemplo:
```ini
[GENERAL]
process_name = Carga diaria cartera
log_file = E:\Proyectos_python\Orquestador_Tareas_Automatizadas\logs\carga.log
dev_mode = false
```

### 2. Etapas `command`
Campos:
- `type`: opcional, por defecto `command`.
- `command`: comando a ejecutar.
- `timeout`: opcional, en segundos.
- `result_key`: opcional, guarda `stdout` para usarlo en otra etapa.
- `condicion`: opcional, evalua si se ejecuta la etapa.
- `show_output`: opcional (`true|false`), incluye salida en resumen.
- `run_as_bat`: opcional (`true|false`), lanza `.bat` en paralelo.

Ejemplo:
```ini
[Validar bandera]
type = command
command = echo true
result_key = continuar

[Proceso principal]
type = command
command = python E:\jobs\main.py --fecha {anio}-{mes}-{dia}
timeout = 1800
condicion = '{continuar}' == 'true'
show_output = true
```

### 3. Etapas `query` (multiples consultas)
Campos:
- `type = query`
- `query_sql`: consulta `SELECT`.
- `query_title`: titulo mostrado en el HTML del correo.
- `max_rows`: maximo 20 (si se pone mas, se recorta a 20).
- `result_key`: opcional, guarda el numero de filas devueltas.
- `condicion`: opcional.

Reglas de seguridad:
- Solo se permite `SELECT`.
- No se permiten DML/DDL (`INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.).
- No se permiten multiples sentencias.

Ejemplo:
```ini
[Consulta resumen]
type = query
query_title = Resumen de ejecuciones
query_sql = SELECT FECEJEC, PROCESO, RESUMEN FROM orquestador.log_procesos
max_rows = 10
result_key = filas_resumen

[Consulta detalle]
type = query
query_title = Detalle de resultados
query_sql = SELECT FECEJEC, PROCESO, RESULTADO FROM orquestador.log_procesos
max_rows = 20
condicion = '{filas_resumen}' != '0'
```

### 4. Ejemplo completo de `.ini`
```ini
[GENERAL]
process_name = Proceso de prueba
log_file = E:\Proyectos_python\Orquestador_Tareas_Automatizadas\test\logs\log_proceso.log
dev_mode = false

[Impresion consola]
command = echo true
result_key = continuar

[Consulta resumen]
type = query
query_title = Resumen de ejecuciones
query_sql = SELECT FECEJEC, PROCESO, RESUMEN FROM orquestador.log_procesos
max_rows = 10

[Espera]
command = timeout /t 10 /nobreak >nul
condicion = '{continuar}' == 'true'

[Lanzar BAT paralelo]
command = E:\Proyectos_python\Orquestador_Tareas_Automatizadas\test\bat_paralelo.bat
run_as_bat = true
```

## Manual del archivo `.bat`
El `.bat` sirve para lanzar el pipeline con una configuracion especifica y registrar el path del propio `.bat`.

### 1. Estructura recomendada
```bat
@echo off
setlocal

set "ROOT=E:\Proyectos_python\Orquestador_Tareas_Automatizadas"
set "VENV=%ROOT%\.venv\Scripts\python.exe"
set "CONFIG=%ROOT%\test\configuracion.ini"

"%VENV%" "%ROOT%\pipeline_proceso.py" --config "%CONFIG%" --batpath "%~f0"
set "EXIT_CODE=%ERRORLEVEL%"

echo Finalizo con codigo %EXIT_CODE%
exit /b %EXIT_CODE%
```

### 2. Buenas practicas para `.bat`
- Usar rutas absolutas.
- Pasar siempre `--batpath "%~f0"` para trazabilidad.
- Propagar `ERRORLEVEL` con `exit /b %EXIT_CODE%`.
- Evitar comandos interactivos.

## Ejecucion
Desde terminal:
```powershell
python .\pipeline_proceso.py --config .\test\configuracion.ini
```

O con `.bat`:
```powershell
.\test\proceso.bat
```

## Salida esperada
- Log local en la ruta definida por `log_file`.
- Resumen en tabla `orquestador.log_procesos` (si `dev_mode=false`).
- Un unico POST a Power Automate con:
- metadatos de ejecucion,
- `log_summary`,
- `query_sections` (cada consulta con HTML).

## Integracion con Power Automate
- Schema sugerido del trigger HTTP:
- [power_automate_http_schema.json](E:/Proyectos_python/Orquestador_Tareas_Automatizadas/test/power_automate_http_schema.json)
- En el correo, iterar `query_sections` y concatenar `html`.

## Troubleshooting rapido
- No conecta a BD:
- validar `DB_URL` o modulo corporativo de conexion.
- No envia correo:
- validar `URL_FLOW` y conectividad saliente.
- Query rechazada:
- revisar que sea `SELECT` simple y sin sentencias peligrosas.
- No aparecen consultas en correo:
- verificar `type=query` en secciones del `.ini`.

