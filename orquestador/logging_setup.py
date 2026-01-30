import logging
from logging.handlers import RotatingFileHandler

from orquestador.constants import LOG_FORMAT, DEFAULT_DATE_FORMAT, DEFAULT_ENCODING

def setup_logging(log_file: str, level: int = logging.INFO, *, max_bytes: int = 5*1024 * 1024, backup_count: int = 5) -> logging.Logger:
    """
    Configurar logging en ROOT logger (logging.info/ warning / error) de forma idempotente.
    - Consola + RotatingFileHandler
    - Evita duplicar handlers si se llama más de una vez
    - Expone la ruta del log como logger.log_file_path
    """
    
    logger = logging.getLogger()  # ROOT

    # Si ya hay handlers, no vuelvas a agregar (evita logs duplicados)
    if logger.handlers:
        logger.setLevel(level)
        setattr(logger, "log_file_path", log_file)
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding=DEFAULT_ENCODING,
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    setattr(logger, "log_file_path", file_handler.baseFilename)
    
    return logger
