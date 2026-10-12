"""
Módulo centralizado de logging para el proceso ETL Airbnb.

Proporciona una configuración consistente de logs para todos los scripts del ETL,
generando un archivo por ejecución con formato: logs/log_YYYYMMDD_HHMM.txt
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class ETLLogger:
    """
    Clase singleton para manejo centralizado de logs en el proceso ETL.

    Características:
    - Un archivo de log por ejecución (timestamp en nombre)
    - Niveles: INFO, WARNING, ERROR
    - Formato: timestamp | nivel | módulo | mensaje
    - Directorio logs/ creado automáticamente
    """

    _instance: Optional['ETLLogger'] = None
    _logger: Optional[logging.Logger] = None
    _log_file: Optional[str] = None

    def __new__(cls) -> 'ETLLogger':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._logger is None:
            self._setup_logger()

    def _setup_logger(self) -> None:
        """Configura el logger con archivo y consola."""
        # Crear directorio de logs si no existe
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        self._log_file = log_dir / f"log_{timestamp}.txt"

        # Configurar logger
        self._logger = logging.getLogger("ETL_Airbnb")
        self._logger.setLevel(logging.DEBUG)

        # Evitar duplicar handlers si ya existen
        if not self._logger.handlers:
            # Handler para archivo
            file_handler = logging.FileHandler(self._log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)

            # Handler para consola
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_format = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%H:%M:%S'
            )
            console_handler.setFormatter(console_format)

            self._logger.addHandler(file_handler)
            self._logger.addHandler(console_handler)

    def get_logger(self) -> logging.Logger:
        """Retorna la instancia del logger configurado."""
        return self._logger

    def get_log_file(self) -> str:
        """Retorna la ruta del archivo de log actual."""
        return str(self._log_file) if self._log_file else ""

    def info(self, message: str, module: str = "") -> None:
        """Registra mensaje de nivel INFO."""
        msg = f"[{module}] {message}" if module else message
        self._logger.info(msg)

    def warning(self, message: str, module: str = "") -> None:
        """Registra mensaje de nivel WARNING."""
        msg = f"[{module}] {message}" if module else message
        self._logger.warning(msg)

    def error(self, message: str, module: str = "") -> None:
        """Registra mensaje de nivel ERROR."""
        msg = f"[{module}] {message}" if module else message
        self._logger.error(msg)

    def debug(self, message: str, module: str = "") -> None:
        """Registra mensaje de nivel DEBUG."""
        msg = f"[{module}] {message}" if module else message
        self._logger.debug(msg)


# Instancia global para uso directo
logger = ETLLogger()


def get_logger() -> logging.Logger:
    """Función de conveniencia para obtener el logger estándar."""
    return logger.get_logger()


def log_info(message: str, module: str = "") -> None:
    """Función de conveniencia para log INFO."""
    logger.info(message, module)


def log_warning(message: str, module: str = "") -> None:
    """Función de conveniencia para log WARNING."""
    logger.warning(message, module)


def log_error(message: str, module: str = "") -> None:
    """Función de conveniencia para log ERROR."""
    logger.error(message, module)