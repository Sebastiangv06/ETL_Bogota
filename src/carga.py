"""
Módulo de Carga - ETL Airbnb Bogotá

Clase Carga para insertar datos transformados en SQLite y exportar a XLSX
con verificación y registro de logs.
"""

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys
import os

# Agregar src al path para importar logger
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from logger import log_info, log_warning, log_error, get_logger


class Carga:
    """
    Clase para la carga de datos transformados.

    Permite:
    - Insertar datos en base de datos SQLite
    - Exportar datos a archivos XLSX (Excel)
    - Verificar que los registros se hayan cargado correctamente
    - Registrar logs de eventos principales del proceso
    """

    def __init__(
        self,
        sqlite_path: str = "data/airbnb_etl.db",
        xlsx_dir: str = "data/xlsx_export"
    ) -> None:
        """
        Inicializa la configuración de carga.

        Args:
            sqlite_path: Ruta del archivo SQLite (default: data/airbnb_etl.db).
            xlsx_dir: Directorio para exportar archivos XLSX (default: data/xlsx_export).
        """
        self.sqlite_path = sqlite_path
        self.xlsx_dir = Path(xlsx_dir)
        self._engine: Optional[Engine] = None
        self.logger = get_logger()

        # Crear directorios necesarios
        Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        self.xlsx_dir.mkdir(parents=True, exist_ok=True)

    def _get_engine(self) -> Engine:
        """Obtiene o crea el engine de SQLAlchemy para SQLite."""
        if self._engine is None:
            self._engine = create_engine(f"sqlite:///{self.sqlite_path}", echo=False)
        return self._engine

    def cerrar_conexion(self) -> None:
        """Cierra la conexión a la base de datos."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            log_info("Conexión SQLite cerrada", "Carga")

    # ==================== CARGA A SQLITE ====================

    def _sanitizar_dataframe_sqlite(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitiza un DataFrame para compatibilidad con SQLite.

        - Convierte columnas 'category' a string
        - Convierte datetime64 a string ISO
        - Convierte boolean a int
        """
        df = df.copy()

        for col in df.columns:
            dtype = df[col].dtype

            # Category -> string
            if dtype.name == 'category':
                df[col] = df[col].astype(str)

            # Datetime -> string ISO
            elif dtype == 'datetime64[us]' or dtype == 'datetime64[ns]':
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Boolean -> int (SQLite no tiene tipo boolean nativo)
            elif dtype == 'bool':
                df[col] = df[col].astype(int)

        return df

    def cargar_a_sqlite(
        self,
        datos: Dict[str, pd.DataFrame],
        if_exists: str = 'replace',
        chunksize: int = 1000
    ) -> Dict[str, int]:
        """
        Carga múltiples DataFrames a tablas en SQLite.

        Args:
            datos: Diccionario {nombre_tabla: DataFrame}.
            if_exists: 'fail', 'replace', 'append' (default: replace).
            chunksize: Tamaño de lote para inserción (default: 1000).

        Returns:
            Dict[str, int]: Registros insertados por tabla.
        """
        if not datos:
            log_warning("No hay datos para cargar a SQLite", "Carga")
            return {}

        engine = self._get_engine()
        resultados = {}

        log_info(f"Iniciando carga a SQLite: {self.sqlite_path}", "Carga")

        for nombre_tabla, df in datos.items():
            if df.empty:
                log_warning(f"Tabla '{nombre_tabla}': DataFrame vacío, omitiendo", "Carga")
                resultados[nombre_tabla] = 0
                continue

            try:
                # Sanitizar nombre de tabla
                tabla_limpia = self._sanitizar_nombre_tabla(nombre_tabla)

                # Sanitizar DataFrame para SQLite
                df_limpio = self._sanitizar_dataframe_sqlite(df)

                # Cargar a SQLite (method=None evita límite de 999 variables de SQLite)
                registros_antes = len(df_limpio)
                df_limpio.to_sql(
                    name=tabla_limpia,
                    con=engine,
                    if_exists=if_exists,
                    index=False,
                    chunksize=chunksize,
                    method=None  # 'multi' causa error 'too many SQL variables' con muchas columnas
                )

                # Verificar carga
                registros_despues = self._contar_registros_tabla(tabla_limpia)
                resultados[nombre_tabla] = registros_despues

                log_info(
                    f"Cargada tabla '{tabla_limpia}': {registros_despues} registros "
                    f"(DataFrame tenía {registros_antes})",
                    "Carga"
                )

                if registros_antes != registros_despues:
                    log_warning(
                        f"Discrepancia en '{tabla_limpia}': "
                        f"DataFrame={registros_antes}, SQLite={registros_despues}",
                        "Carga"
                    )

            except Exception as e:
                log_error(f"Error cargando '{nombre_tabla}' a SQLite: {str(e)}", "Carga")
                resultados[nombre_tabla] = 0

        total = sum(resultados.values())
        log_info(f"Carga a SQLite completada: {total} registros totales en {len(resultados)} tablas", "Carga")

        return resultados

    def _sanitizar_nombre_tabla(self, nombre: str) -> str:
        """Sanitiza nombre de tabla para SQLite."""
        # Reemplazar caracteres no válidos
        nombre = nombre.lower().strip()
        nombre = nombre.replace(' ', '_').replace('-', '_')
        nombre = ''.join(c for c in nombre if c.isalnum() or c == '_')
        # Asegurar que no empiece con número
        if nombre and nombre[0].isdigit():
            nombre = 't_' + nombre
        return nombre or 'tabla'

    def _contar_registros_tabla(self, tabla: str) -> int:
        """Cuenta registros en una tabla de SQLite."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}"))
                return result.scalar() or 0
        except Exception:
            return 0

    def verificar_carga_sqlite(
        self,
        tablas_esperadas: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Verifica que las tablas se hayan cargado correctamente en SQLite.

        Args:
            tablas_esperadas: Lista de nombres de tablas esperadas (opcional).

        Returns:
            Dict: Reporte de verificación.
        """
        engine = self._get_engine()
        inspector = inspect(engine)

        tablas_reales = inspector.get_table_names()

        if tablas_esperadas is None:
            tablas_esperadas = tablas_reales

        reporte = {
            'sqlite_path': self.sqlite_path,
            'tablas_encontradas': tablas_reales,
            'tablas_esperadas': tablas_esperadas,
            'verificacion': {},
            'total_registros': 0,
            'exitoso': True
        }

        for tabla in tablas_esperadas:
            tabla_limpia = self._sanitizar_nombre_tabla(tabla)

            if tabla_limpia in tablas_reales:
                count = self._contar_registros_tabla(tabla_limpia)
                columnas = [c['name'] for c in inspector.get_columns(tabla_limpia)]

                reporte['verificacion'][tabla] = {
                    'existe': True,
                    'registros': count,
                    'columnas': len(columnas),
                    'nombres_columnas': columnas
                }
                reporte['total_registros'] += count

                log_info(f"Verificación OK: '{tabla_limpia}' - {count} registros, {len(columnas)} columnas", "Carga")
            else:
                reporte['verificacion'][tabla] = {
                    'existe': False,
                    'error': f"Tabla '{tabla_limpia}' no encontrada en SQLite"
                }
                reporte['exitoso'] = False
                log_error(f"Verificación FALLÓ: '{tabla_limpia}' no existe", "Carga")

        log_info(f"Verificación SQLite: {'EXITOSA' if reporte['exitoso'] else 'CON ERRORES'} - {reporte['total_registros']} registros totales", "Carga")

        return reporte

    def ejecutar_consulta_sql(self, query: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL arbitraria en la base de datos SQLite.

        Args:
            query: Consulta SQL a ejecutar.

        Returns:
            pd.DataFrame: Resultado de la consulta.
        """
        try:
            engine = self._get_engine()
            return pd.read_sql(query, engine)
        except Exception as e:
            log_error(f"Error ejecutando consulta SQL: {str(e)}", "Carga")
            return pd.DataFrame()

    # ==================== EXPORTACIÓN A XLSX ====================

    # Límite de filas por hoja en Excel (XLSX)
    # Restamos 1 para la fila de encabezado
    MAX_EXCEL_ROWS = 1048575

    def exportar_a_xlsx(
        self,
        datos: Dict[str, pd.DataFrame],
        nombre_archivo: str = "airbnb_datos_transformados.xlsx",
        hoja_por_tabla: bool = True,
        incluir_indice: bool = False
    ) -> str:
        """
        Exporta DataFrames a archivo XLSX (Excel).
        Divide automáticamente DataFrames que exceden el límite de filas de Excel (1,048,576)
        en múltiples hojas con sufijo numérico.

        Args:
            datos: Diccionario {nombre_hoja: DataFrame}.
            nombre_archivo: Nombre del archivo XLSX.
            hoja_por_tabla: Si True, una hoja por DataFrame. Si False, todos en una hoja.
            incluir_indice: Si incluir índice en la exportación.

        Returns:
            str: Ruta completa del archivo generado.
        """
        if not datos:
            log_warning("No hay datos para exportar a XLSX", "Carga")
            return ""

        ruta_completa = self.xlsx_dir / nombre_archivo

        try:
            # Si alguna tabla no cabe en una hoja (ej. Calendar, ~7M filas) se usa
            # xlsxwriter en modo constant_memory: escribe fila a fila directo a
            # disco en vez de armar todo el libro en RAM como hace openpyxl.
            grande = any(len(v) > self.MAX_EXCEL_ROWS for v in datos.values())
            if grande:
                log_info("Tabla grande detectada: usando xlsxwriter (constant_memory)", "Carga")
                writer_ctx = pd.ExcelWriter(
                    ruta_completa,
                    engine='xlsxwriter',
                    engine_kwargs={'options': {'constant_memory': True}}
                )
            else:
                writer_ctx = pd.ExcelWriter(ruta_completa, engine='openpyxl')

            with writer_ctx as writer:
                if hoja_por_tabla:
                    # Una hoja por DataFrame (con división si excede límite)
                    for nombre_hoja, df in datos.items():
                        if df.empty:
                            log_warning(f"Hoja '{nombre_hoja}': DataFrame vacío", "Carga")
                            continue

                        self._exportar_dataframe_particionado(
                            writer, nombre_hoja, df, incluir_indice
                        )
                else:
                    # Todos en una hoja (concatenar con identificador)
                    dfs_con_origen = []
                    for nombre, df in datos.items():
                        if not df.empty:
                            df_copia = df.copy()
                            df_copia['_origen'] = nombre
                            dfs_con_origen.append(df_copia)

                    if dfs_con_origen:
                        df_combinado = pd.concat(dfs_con_origen, ignore_index=True)
                        self._exportar_dataframe_particionado(
                            writer, 'Datos_Combinados', df_combinado, incluir_indice
                        )

            log_info(f"Exportación XLSX completada: {ruta_completa}", "Carga")
            return str(ruta_completa)

        except Exception as e:
            log_error(f"Error exportando a XLSX: {str(e)}", "Carga")
            return ""

    def _exportar_dataframe_particionado(
        self,
        writer: pd.ExcelWriter,
        nombre_base: str,
        df: pd.DataFrame,
        incluir_indice: bool
    ) -> None:
        """
        Exporta un DataFrame a Excel, particionándolo en múltiples hojas
        si excede el límite de filas de Excel.
        """
        hoja_base = self._sanitizar_nombre_hoja(nombre_base)
        total_filas = len(df)

        if total_filas <= self.MAX_EXCEL_ROWS:
            # Cabe en una sola hoja
            df.to_excel(writer, sheet_name=hoja_base, index=incluir_indice)
            self._autoajustar_columnas(writer, hoja_base, df)
            log_info(
                f"Exportada hoja '{hoja_base}': {total_filas} filas, {len(df.columns)} columnas",
                "Carga"
            )
        else:
            # Dividir en múltiples hojas
            num_hojas = (total_filas + self.MAX_EXCEL_ROWS - 1) // self.MAX_EXCEL_ROWS
            log_info(
                f"Dividiendo '{hoja_base}' en {num_hojas} hojas "
                f"({total_filas} filas, límite {self.MAX_EXCEL_ROWS} por hoja)",
                "Carga"
            )

            for i in range(num_hojas):
                inicio = i * self.MAX_EXCEL_ROWS
                fin = min((i + 1) * self.MAX_EXCEL_ROWS, total_filas)
                df_particion = df.iloc[inicio:fin]

                # Nombre de hoja con sufijo (máx 31 chars)
                sufijo = f"_{i+1}"
                hoja_nombre = hoja_base[:31 - len(sufijo)] + sufijo

                df_particion.to_excel(writer, sheet_name=hoja_nombre, index=incluir_indice)
                self._autoajustar_columnas(writer, hoja_nombre, df_particion)

                log_info(
                    f"Exportada hoja '{hoja_nombre}': {len(df_particion)} filas "
                    f"({inicio+1}-{fin} de {total_filas})",
                    "Carga"
                )

    def _sanitizar_nombre_hoja(self, nombre: str) -> str:
        """Sanitiza nombre de hoja para Excel (max 31 chars)."""
        # Caracteres no permitidos: [ ] : * ? / \
        nombre = str(nombre)
        for c in ['[', ']', ':', '*', '?', '/', '\\']:
            nombre = nombre.replace(c, '_')
        return nombre[:31] if len(nombre) > 31 else nombre

    def _autoajustar_columnas(self, writer: pd.ExcelWriter, hoja: str, df: pd.DataFrame) -> None:
        """Auto-ajusta el ancho de columnas en una hoja Excel."""
        try:
            # Omitir auto-ajuste para DataFrames muy grandes (>100K filas) por rendimiento
            if len(df) > 100000:
                return

            worksheet = writer.sheets[hoja]
            for idx, col in enumerate(df.columns):
                # Calcular ancho basado en contenido (máximo 50)
                # Usar muestra para DataFrames medianos para mejorar rendimiento
                if len(df) > 10000:
                    muestra = df[col].astype(str).head(10000)
                    max_len = max(muestra.map(len).max(), len(str(col)))
                else:
                    max_len = max(
                        df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                        len(str(col))
                    )
                ancho = min(max_len + 2, 50)
                # Convertir índice a letra de columna Excel (A, B, ..., Z, AA, AB, ...)
                col_letter = self._indice_a_columna_excel(idx)
                worksheet.column_dimensions[col_letter].width = ancho
        except Exception:
            # Silencioso si falla el auto-ajuste
            pass

    def _indice_a_columna_excel(self, idx: int) -> str:
        """
        Convierte un índice base 0 a letra de columna Excel.
        0 -> A, 25 -> Z, 26 -> AA, 27 -> AB, etc.
        """
        resultado = []
        while idx >= 0:
            resultado.append(chr(65 + (idx % 26)))
            idx = idx // 26 - 1
        return ''.join(reversed(resultado))

    def exportar_multiples_xlsx(
        self,
        datos: Dict[str, pd.DataFrame],
        prefijo: str = "airbnb_",
        resumen_tablas_grandes: bool = False
    ) -> Dict[str, str]:
        """
        Exporta cada DataFrame a un archivo XLSX separado.

        Args:
            datos: Diccionario {nombre: DataFrame}.
            prefijo: Prefijo para nombres de archivo.
            resumen_tablas_grandes: Si True, las tablas que no caben en una hoja
                se exportan como CSV + resumen mensual en vez de varias hojas.

        Returns:
            Dict[str, str]: {nombre_tabla: ruta_archivo}.
        """
        rutas = {}

        for nombre, df in datos.items():
            if df.empty:
                continue

            base = f"{prefijo}{self._sanitizar_nombre_tabla(nombre)}"

            # Opcional (resumen_tablas_grandes=True): en vez de partir en hojas,
            # exporta CSV completo + XLSX con un resumen mensual.
            if resumen_tablas_grandes and len(df) > self.MAX_EXCEL_ROWS:
                ruta_csv = self.xlsx_dir / f"{base}.csv"
                try:
                    df.to_csv(ruta_csv, index=False, encoding='utf-8-sig', chunksize=200000)
                    log_info(
                        f"'{nombre}' excede el límite de Excel ({len(df)} filas): "
                        f"exportada completa a CSV: {ruta_csv}", "Carga"
                    )
                    rutas[f"{nombre}_csv"] = str(ruta_csv)
                except Exception as e:
                    log_error(f"Error exportando '{nombre}' a CSV: {str(e)}", "Carga")

                resumen = self._resumir_tabla_grande(df)
                if not resumen.empty:
                    ruta = self.exportar_a_xlsx(
                        {f"{nombre}_resumen": resumen}, f"{base}_resumen.xlsx"
                    )
                    if ruta:
                        rutas[f"{nombre}_resumen"] = ruta
                continue

            ruta = self.exportar_a_xlsx({nombre: df}, f"{base}.xlsx")
            if ruta:
                rutas[nombre] = ruta

        log_info(f"Exportación múltiple XLSX: {len(rutas)} archivos generados", "Carga")
        return rutas

    def _resumir_tabla_grande(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resumen mensual de una tabla tipo Calendar (listing_id, date, available,
        price_normalized...). Devuelve un DataFrame pequeño que sí cabe en Excel.
        """
        if 'date' not in df.columns:
            log_warning("Resumen omitido: la tabla no tiene columna 'date'", "Carga")
            return pd.DataFrame()

        try:
            periodo = df['date'].astype(str).str[:7].rename('anio_mes')  # YYYY-MM
            agrupado = df.groupby(periodo)
            resumen = pd.DataFrame({'registros': agrupado.size()})

            if 'listing_id' in df.columns:
                resumen['alojamientos'] = agrupado['listing_id'].nunique()
            if 'available' in df.columns:
                resumen['pct_disponible'] = (agrupado['available'].mean() * 100).round(2)
            for col in ('price_normalized', 'price'):
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    resumen['precio_promedio'] = agrupado[col].mean().round(0)
                    resumen['precio_mediano'] = agrupado[col].median().round(0)
                    break

            return resumen.reset_index()
        except Exception as e:
            log_error(f"Error generando resumen de tabla grande: {str(e)}", "Carga")
            return pd.DataFrame()

    # ==================== PIPELINE COMPLETO ====================

    def cargar_todo(
        self,
        datos: Dict[str, pd.DataFrame],
        exportar_xlsx: bool = True,
        nombre_xlsx: str = "airbnb_datos_transformados.xlsx",
        xlsx_por_tabla: bool = False
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo de carga: SQLite + XLSX + Verificación.

        Args:
            datos: Diccionario con DataFrames transformados.
            exportar_xlsx: Si exportar también a XLSX.
            nombre_xlsx: Nombre del archivo XLSX principal.
            xlsx_por_tabla: Si generar un XLSX por tabla además del principal.

        Returns:
            Dict: Reporte completo de la carga.
        """
        log_info("=== INICIANDO PIPELINE COMPLETO DE CARGA ===", "Carga")

        reporte = {
            'sqlite': {},
            'xlsx_principal': '',
            'xlsx_por_tabla': {},
            'verificacion': {},
            'exitoso': True
        }

        # 1. Cargar a SQLite
        reporte['sqlite'] = self.cargar_a_sqlite(datos)

        # 2. Verificar carga en SQLite
        reporte['verificacion'] = self.verificar_carga_sqlite(list(datos.keys()))
        reporte['exitoso'] = reporte['verificacion']['exitoso']

        # 3. Exportar a XLSX (archivo principal con tablas que caben en Excel)
        # Excluir tablas > 1M filas (límite práctico Excel: 1,048,576 filas/hoja)
        if exportar_xlsx:
            datos_xlsx = {k: v for k, v in datos.items() if len(v) <= 1000000}
            excluidas = [k for k, v in datos.items() if len(v) > 1000000]
            if excluidas:
                log_info(f"Excluidas del XLSX principal (exceden 1M filas): {excluidas}", "Carga")
            if datos_xlsx:
                reporte['xlsx_principal'] = self.exportar_a_xlsx(datos_xlsx, nombre_xlsx, hoja_por_tabla=True)
            else:
                log_warning("Ninguna tabla cabe en XLSX principal (todas > 1M filas)", "Carga")
                reporte['xlsx_principal'] = ""

        # 4. Exportar XLSX individuales por tabla (opcional, con partición automática)
        if xlsx_por_tabla:
            reporte['xlsx_por_tabla'] = self.exportar_multiples_xlsx(datos)

        log_info("=== PIPELINE DE CARGA COMPLETADO ===", "Carga")
        log_info(f"SQLite: {sum(reporte['sqlite'].values())} registros en {len(reporte['sqlite'])} tablas", "Carga")
        if reporte['xlsx_principal']:
            log_info(f"XLSX principal: {reporte['xlsx_principal']}", "Carga")
        if reporte['xlsx_por_tabla']:
            log_info(f"XLSX por tabla: {len(reporte['xlsx_por_tabla'])} archivos", "Carga")

        return reporte

    def generar_reporte_carga(self, reporte: Dict) -> str:
        """
        Genera un reporte textual del proceso de carga.

        Args:
            reporte: Diccionario retornado por cargar_todo().

        Returns:
            str: Reporte formateado.
        """
        lineas = ["=" * 60]
        lineas.append("REPORTE DE CARGA - ETL AIRBNB")
        lineas.append("=" * 60)

        lineas.append(f"\nBase de datos SQLite: {self.sqlite_path}")
        lineas.append(f"Directorio XLSX: {self.xlsx_dir}")

        lineas.append("\n--- CARGA SQLITE ---")
        for tabla, count in reporte.get('sqlite', {}).items():
            lineas.append(f"  {tabla}: {count} registros")

        lineas.append("\n--- VERIFICACIÓN SQLITE ---")
        verif = reporte.get('verificacion', {})
        lineas.append(f"  Estado: {'EXITOSO' if verif.get('exitoso') else 'CON ERRORES'}")
        lineas.append(f"  Total registros: {verif.get('total_registros', 0)}")

        for tabla, info in verif.get('verificacion', {}).items():
            if info.get('existe'):
                lineas.append(f"  [OK] {tabla}: {info['registros']} regs, {info['columnas']} cols")
            else:
                lineas.append(f"  [ERROR] {tabla}: {info.get('error', 'No encontrada')}")

        if reporte.get('xlsx_principal'):
            lineas.append(f"\n--- XLSX PRINCIPAL ---")
            lineas.append(f"  {reporte['xlsx_principal']}")

        if reporte.get('xlsx_por_tabla'):
            lineas.append(f"\n--- XLSX POR TABLA ---")
            for tabla, ruta in reporte['xlsx_por_tabla'].items():
                lineas.append(f"  {tabla}: {ruta}")

        return "\n".join(lineas)


if __name__ == "__main__":
    # Prueba rápida con datos sintéticos
    print("=== Prueba de Carga ===")

    # Crear datos de prueba
    df_listings = pd.DataFrame({
        'id': [1, 2, 3],
        'price_normalized': [100.0, 200.5, 300.0],
        'price_category': ['Bajo', 'Medio', 'Alto'],
        'room_type': ['Entire home', 'Private room', 'Shared room'],
        'year': [2023, 2023, 2023],
        'month': [1, 2, 3]
    })

    df_reviews = pd.DataFrame({
        'listing_id': [1, 1, 2],
        'reviewer_id': [10, 11, 12],
        'date': ['2023-01-20', '2023-02-15', '2023-03-01'],
        'review_year': [2023, 2023, 2023],
        'review_month': [1, 2, 3]
    })

    df_calendar = pd.DataFrame({
        'listing_id': [1, 1, 2],
        'date': ['2023-06-01', '2023-06-02', '2023-06-01'],
        'price_normalized': [100.0, 120.0, 80.0],
        'available': [True, False, True],
        'cal_year': [2023, 2023, 2023],
        'cal_month': [6, 6, 6]
    })

    datos = {'Listings': df_listings, 'Reviews': df_reviews, 'Calendar': df_calendar}

    # Cargar
    carga = Carga(sqlite_path="data/test_airbnb.db", xlsx_dir="data/test_xlsx")
    reporte = carga.cargar_todo(datos, exportar_xlsx=True, xlsx_por_tabla=True)

    print(carga.generar_reporte_carga(reporte))

    # Verificar con consulta
    print("\n--- Consulta de prueba ---")
    result = carga.ejecutar_consulta_sql("SELECT * FROM listings LIMIT 5")
    print(result)