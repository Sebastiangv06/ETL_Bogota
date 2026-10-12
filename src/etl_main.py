#!/usr/bin/env python3
"""
Script principal del proceso ETL - Airbnb Bogotá

Orquesta el pipeline completo: Extraccion -> Transformacion -> Carga
con logging centralizado y manejo de errores.
"""

import sys
import os
from typing import Dict, Optional

# Agregar src al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extraccion import Extraccion, extraer_datos_airbnb
from transformacion import Transformacion
from carga import Carga
from logger import log_info, log_warning, log_error, logger


def ejecutar_etl_completo(
    mongo_host: str = "localhost",
    mongo_port: int = 27017,
    mongo_db: str = "airbnb_bogota",
    colecciones: Optional[list] = None,
    sqlite_path: str = "data/airbnb_etl.db",
    xlsx_dir: str = "data/xlsx_export",
    xlsx_principal: str = "airbnb_datos_transformados.xlsx"
) -> Dict:
    """
    Ejecuta el pipeline ETL completo.

    Args:
        mongo_host: Host de MongoDB.
        mongo_port: Puerto de MongoDB.
        mongo_db: Nombre de la base de datos MongoDB.
        colecciones: Lista de colecciones a extraer (None = todas estándar).
        sqlite_path: Ruta del archivo SQLite de salida.
        xlsx_dir: Directorio para archivos XLSX.
        xlsx_principal: Nombre del archivo XLSX principal.

    Returns:
        Dict: Reporte completo del proceso.
    """
    reporte = {
        'extraccion': {},
        'transformacion': {},
        'carga': {},
        'exitoso': False,
        'errores': []
    }

    log_info("=" * 60, "ETL_Main")
    log_info("INICIANDO PROCESO ETL COMPLETO - AIRBNB BOGOTÁ", "ETL_Main")
    log_info("=" * 60, "ETL_Main")

    try:
        # ==================== FASE 1: EXTRACCIÓN ====================
        log_info(">>> FASE 1: EXTRACCIÓN DE DATOS DESDE MONGODB <<<", "ETL_Main")

        with Extraccion(mongo_host, mongo_port, mongo_db) as extractor:
            if not extractor.verificar_conexion():
                raise ConnectionError(
                    f"No se pudo conectar a MongoDB en {mongo_host}:{mongo_port}. "
                    "Verifique que MongoDB esté corriendo y la base de datos exista."
                )

            # Listar colecciones disponibles
            cols_disponibles = extractor.listar_colecciones()
            log_info(f"Colecciones disponibles en MongoDB: {cols_disponibles}", "ETL_Main")

            # Determinar colecciones a extraer
            if colecciones is None:
                colecciones_a_extraer = ["Listings", "Reviews", "Calendar"]
            else:
                colecciones_a_extraer = colecciones

            # Filtrar solo las que existen
            colecciones_a_extraer = [c for c in colecciones_a_extraer if c in cols_disponibles]

            if not colecciones_a_extraer:
                raise ValueError(
                    f"Ninguna de las colecciones solicitadas existe en MongoDB. "
                    f"Disponibles: {cols_disponibles}"
                )

            # Extraer datos
            datos_crudos = extractor.extraer_todas(colecciones_a_extraer)

            # Verificar que se obtuvieron datos
            total_extraidos = sum(len(df) for df in datos_crudos.values())
            if total_extraidos == 0:
                raise ValueError("No se extrajeron datos de ninguna colección")

            reporte['extraccion'] = {
                'colecciones': list(datos_crudos.keys()),
                'registros_por_coleccion': {k: len(v) for k, v in datos_crudos.items()},
                'total_registros': total_extraidos
            }

            log_info(f"Extracción completada: {total_extraidos} registros totales", "ETL_Main")

        # ==================== FASE 2: TRANSFORMACIÓN ====================
        log_info(">>> FASE 2: TRANSFORMACIÓN Y LIMPIEZA DE DATOS <<<", "ETL_Main")

        transformador = Transformacion()
        datos_transformados = transformador.transformar_todo(datos_crudos)

        reporte['transformacion'] = {
            'registros_por_coleccion': {k: len(v) for k, v in datos_transformados.items()},
            'columnas_por_coleccion': {k: len(v.columns) for k, v in datos_transformados.items()},
            'estadisticas': transformador.obtener_estadisticas()
        }

        # Mostrar reporte de transformación
        print("\n" + transformador.generar_reporte_transformacion())

        # ==================== FASE 3: CARGA ====================
        log_info(">>> FASE 3: CARGA A SQLITE Y EXPORTACIÓN XLSX <<<", "ETL_Main")

        cargador = Carga(sqlite_path=sqlite_path, xlsx_dir=xlsx_dir)
        reporte_carga = cargador.cargar_todo(
            datos_transformados,
            exportar_xlsx=True,
            nombre_xlsx=xlsx_principal,
            xlsx_por_tabla=True
        )

        reporte['carga'] = reporte_carga

        # Mostrar reporte de carga
        print("\n" + cargador.generar_reporte_carga(reporte_carga))

        # ==================== RESUMEN FINAL ====================
        reporte['exitoso'] = reporte_carga.get('exitoso', False)

        log_info("=" * 60, "ETL_Main")
        log_info("PROCESO ETL COMPLETADO EXITOSAMENTE", "ETL_Main")
        log_info("=" * 60, "ETL_Main")

        log_info(f"Archivo de log: {logger.get_log_file()}", "ETL_Main")
        log_info(f"SQLite: {sqlite_path}", "ETL_Main")
        log_info(f"XLSX: {xlsx_dir}/{xlsx_principal}", "ETL_Main")

    except Exception as e:
        error_msg = f"Error en proceso ETL: {str(e)}"
        log_error(error_msg, "ETL_Main")
        reporte['errores'].append(error_msg)
        reporte['exitoso'] = False
        raise

    finally:
        # Cerrar conexiones
        try:
            Carga(sqlite_path).cerrar_conexion()
        except Exception:
            pass

    return reporte


def main():
    """Función principal para ejecución desde línea de comandos."""
    import argparse

    parser = argparse.ArgumentParser(description="ETL Airbnb Bogotá - Pipeline completo")
    parser.add_argument('--mongo-host', default='localhost', help='Host MongoDB')
    parser.add_argument('--mongo-port', type=int, default=27017, help='Puerto MongoDB')
    parser.add_argument('--mongo-db', default='airbnb_bogota', help='Base de datos MongoDB')
    parser.add_argument('--sqlite', default='data/airbnb_etl.db', help='Archivo SQLite salida')
    parser.add_argument('--xlsx-dir', default='data/xlsx_export', help='Directorio XLSX')
    parser.add_argument('--xlsx-file', default='airbnb_datos_transformados.xlsx', help='Archivo XLSX principal')
    parser.add_argument('--colecciones', nargs='+', help='Colecciones a extraer (default: Listings Reviews Calendar)')

    args = parser.parse_args()

    try:
        reporte = ejecutar_etl_completo(
            mongo_host=args.mongo_host,
            mongo_port=args.mongo_port,
            mongo_db=args.mongo_db,
            colecciones=args.colecciones,
            sqlite_path=args.sqlite,
            xlsx_dir=args.xlsx_dir,
            xlsx_principal=args.xlsx_file
        )

        if reporte['exitoso']:
            print("\n[OK] ETL COMPLETADO EXITOSAMENTE")
            sys.exit(0)
        else:
            print("\n[ERROR] ETL COMPLETADO CON ERRORES")
            sys.exit(1)

    except Exception as e:
        print(f"\n[ERROR CRITICO] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()