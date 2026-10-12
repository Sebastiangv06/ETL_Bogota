"""
Módulo de Transformación - ETL Airbnb Bogotá

Clase Transformacion para limpiar, normalizar y enriquecer los datos extraídos
con registro de logs detallado.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Optional, Tuple, Any
import sys
import os

# Agregar src al path para importar logger
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from logger import log_info, log_warning, log_error, get_logger


class Transformacion:
    """
    Clase para la transformación y limpieza de datos del proceso ETL.

    Implementa:
    - Limpieza de valores nulos y duplicados
    - Normalización de precios (eliminar $, comas, convertir a numérico)
    - Conversión de fechas a formato estándar YYYY-MM-DD
    - Derivación de variables temporales (año, mes, día, trimestre)
    - Categorización de precios por rangos
    - Expansión/tratamiento de campos anidados
    - Generación de DataFrames limpios listos para carga
    """

    def __init__(self) -> None:
        """Inicializa el transformador con contadores y logger."""
        self.logger = get_logger()
        self._stats = {
            'registros_iniciales': {},
            'registros_finales': {},
            'nulos_eliminados': {},
            'duplicados_eliminados': {},
            'transformaciones': []
        }

    def _registrar_transformacion(self, mensaje: str) -> None:
        """Registra una transformación realizada."""
        self._stats['transformaciones'].append(mensaje)
        log_info(mensaje, "Transformacion")

    def _registrar_estadisticas(
        self,
        nombre_df: str,
        inicial: int,
        final: int,
        nulos: int = 0,
        duplicados: int = 0
    ) -> None:
        """Registra estadísticas de transformación."""
        self._stats['registros_iniciales'][nombre_df] = inicial
        self._stats['registros_finales'][nombre_df] = final
        self._stats['nulos_eliminados'][nombre_df] = nulos
        self._stats['duplicados_eliminados'][nombre_df] = duplicados

    # ==================== LIMPIEZA BÁSICA ====================

    def limpiar_nulos(
        self,
        df: pd.DataFrame,
        nombre_df: str = "DataFrame",
        estrategia: str = "eliminar",
        columnas_criticas: Optional[List[str]] = None,
        umbral_nulos: float = 0.5
    ) -> pd.DataFrame:
        """
        Limpia valores nulos del DataFrame.

        Args:
            df: DataFrame a limpiar.
            nombre_df: Nombre identificativo para logs.
            estrategia: 'eliminar' (filas), 'imputar' (media/moda), 'columnas' (dropear cols).
            columnas_criticas: Columnas donde no se permiten nulos.
            umbral_nulos: Umbral (0-1) para eliminar columnas con muchos nulos.

        Returns:
            pd.DataFrame: DataFrame limpio.
        """
        if df.empty:
            log_warning(f"{nombre_df}: DataFrame vacío, omitiendo limpieza de nulos", "Transformacion")
            return df

        registros_iniciales = len(df)
        nulos_antes = df.isnull().sum().sum()

        # Eliminar columnas con muchos nulos
        if estrategia in ['eliminar', 'columnas']:
            cols_muchos_nulos = [
                col for col in df.columns
                if df[col].isnull().mean() > umbral_nulos
            ]
            if cols_muchos_nulos:
                df = df.drop(columns=cols_muchos_nulos)
                self._registrar_transformacion(
                    f"{nombre_df}: Eliminadas {len(cols_muchos_nulos)} columnas "
                    f"con >{umbral_nulos*100}% nulos: {cols_muchos_nulos}"
                )

        # Manejar nulos en columnas críticas
        if columnas_criticas:
            for col in columnas_criticas:
                if col in df.columns:
                    nulos_col = df[col].isnull().sum()
                    if nulos_col > 0:
                        if estrategia == 'eliminar':
                            df = df.dropna(subset=[col])
                        elif estrategia == 'imputar':
                            if df[col].dtype in ['float64', 'int64']:
                                df[col] = df[col].fillna(df[col].median())
                            else:
                                df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else '')

        # Eliminar filas con cualquier nulo restante (estrategia por defecto)
        if estrategia == 'eliminar':
            df = df.dropna()

        registros_finales = len(df)
        nulos_despues = df.isnull().sum().sum()
        nulos_eliminados = nulos_antes - nulos_despues

        self._registrar_estadisticas(
            nombre_df, registros_iniciales, registros_finales,
            nulos=nulos_eliminados
        )

        self._registrar_transformacion(
            f"{nombre_df}: Limpieza nulos - {registros_iniciales} -> {registros_finales} "
            f"registros ({nulos_eliminados} nulos tratados)"
        )

        return df

    def eliminar_duplicados(
        self,
        df: pd.DataFrame,
        nombre_df: str = "DataFrame",
        subset: Optional[List[str]] = None,
        keep: str = 'first'
    ) -> pd.DataFrame:
        """
        Elimina registros duplicados.

        Args:
            df: DataFrame a procesar.
            nombre_df: Nombre identificativo para logs.
            subset: Columnas a considerar para duplicados (None = todas).
            keep: 'first', 'last', o False para eliminar todos.

        Returns:
            pd.DataFrame: DataFrame sin duplicados.
        """
        if df.empty:
            return df

        registros_iniciales = len(df)
        duplicados_antes = df.duplicated(subset=subset).sum()

        if duplicados_antes > 0:
            df = df.drop_duplicates(subset=subset, keep=keep)
            duplicados_eliminados = duplicados_antes
        else:
            duplicados_eliminados = 0

        registros_finales = len(df)

        self._registrar_estadisticas(
            nombre_df, registros_iniciales, registros_finales,
            duplicados=duplicados_eliminados
        )

        if duplicados_eliminados > 0:
            self._registrar_transformacion(
                f"{nombre_df}: Eliminados {duplicados_eliminados} duplicados "
                f"({registros_iniciales} -> {registros_finales})"
            )
        else:
            log_info(f"{nombre_df}: No se encontraron duplicados", "Transformacion")

        return df

    # ==================== NORMALIZACIÓN DE PRECIOS ====================

    def normalizar_precio(
        self,
        df: pd.DataFrame,
        columna_precio: str = 'price',
        nueva_columna: str = 'price_normalized'
    ) -> pd.DataFrame:
        """
        Normaliza columna de precio: elimina $, comas, convierte a numérico.

        Args:
            df: DataFrame con columna de precio.
            columna_precio: Nombre de la columna original de precio.
            nueva_columna: Nombre de la nueva columna normalizada.

        Returns:
            pd.DataFrame: DataFrame con columna de precio normalizada.
        """
        if df.empty or columna_precio not in df.columns:
            log_warning(f"Columna '{columna_precio}' no encontrada en DataFrame", "Transformacion")
            return df

        def limpiar_precio(valor: Any) -> Optional[float]:
            """Limpia un valor de precio individual."""
            if pd.isna(valor):
                return np.nan
            if isinstance(valor, (int, float)):
                return float(valor)
            if isinstance(valor, str):
                # Eliminar $ , espacios y otros símbolos de moneda
                limpio = re.sub(r'[^\d.,\-]', '', valor)
                # Manejar formato con coma como separador decimal o miles
                if ',' in limpio and '.' in limpio:
                    # Formato: 1,234.56 o 1.234,56
                    if limpio.rfind('.') > limpio.rfind(','):
                        # Punto es decimal: 1,234.56
                        limpio = limpio.replace(',', '')
                    else:
                        # Coma es decimal: 1.234,56
                        limpio = limpio.replace('.', '').replace(',', '.')
                elif ',' in limpio:
                    # Solo comas: podría ser separador de miles o decimal
                    if len(limpio.split(',')[-1]) == 2:
                        # Probablemente decimal: 1234,56
                        limpio = limpio.replace(',', '.')
                    else:
                        # Probablemente miles: 1,234
                        limpio = limpio.replace(',', '')
                try:
                    return float(limpio)
                except ValueError:
                    return np.nan
            return np.nan

        df = df.copy()
        df[nueva_columna] = df[columna_precio].apply(limpiar_precio)

        nulos_nuevos = df[nueva_columna].isnull().sum()
        validos = len(df) - nulos_nuevos

        self._registrar_transformacion(
            f"Precio normalizado: '{columna_precio}' -> '{nueva_columna}' "
            f"({validos} válidos, {nulos_nuevos} nulos)"
        )

        return df

    # ==================== CONVERSIÓN DE FECHAS ====================

    def normalizar_fechas(
        self,
        df: pd.DataFrame,
        columnas_fecha: List[str],
        formato_salida: str = '%Y-%m-%d'
    ) -> pd.DataFrame:
        """
        Convierte columnas de fecha a formato estándar YYYY-MM-DD.

        Args:
            df: DataFrame con columnas de fecha.
            columnas_fecha: Lista de nombres de columnas a convertir.
            formato_salida: Formato de salida (default: YYYY-MM-DD).

        Returns:
            pd.DataFrame: DataFrame con fechas normalizadas.
        """
        if df.empty:
            return df

        df = df.copy()

        for col in columnas_fecha:
            if col not in df.columns:
                log_warning(f"Columna de fecha '{col}' no encontrada", "Transformacion")
                continue

            try:
                # Intentar conversión automática
                df[col] = pd.to_datetime(df[col], errors='coerce')

                # Formatear a string YYYY-MM-DD
                df[col] = df[col].dt.strftime(formato_salida)

                nulos = df[col].isnull().sum()
                validos = len(df) - nulos

                self._registrar_transformacion(
                    f"Fecha normalizada: '{col}' -> {formato_salida} "
                    f"({validos} válidas, {nulos} nulas)"
                )

            except Exception as e:
                log_error(f"Error normalizando fecha '{col}': {str(e)}", "Transformacion")

        return df

    def derivar_variables_temporales(
        self,
        df: pd.DataFrame,
        columna_fecha: str = 'date',
        prefijo: str = ''
    ) -> pd.DataFrame:
        """
        Deriva variables temporales a partir de una columna de fecha.

        Crea: año, mes, día, trimestre, día_semana, mes_nombre, es_fin_semana.

        Args:
            df: DataFrame con columna de fecha.
            columna_fecha: Nombre de la columna de fecha (formato YYYY-MM-DD o datetime).
            prefijo: Prefijo para nombres de nuevas columnas (ej: 'calendar_').

        Returns:
            pd.DataFrame: DataFrame con variables temporales derivadas.
        """
        if df.empty or columna_fecha not in df.columns:
            log_warning(f"Columna '{columna_fecha}' no encontrada para derivar variables", "Transformacion")
            return df

        df = df.copy()

        # Asegurar que sea datetime
        try:
            fechas = pd.to_datetime(df[columna_fecha], errors='coerce')
        except Exception:
            log_error(f"No se pudo convertir '{columna_fecha}' a datetime", "Transformacion")
            return df

        nulos = fechas.isnull().sum()
        if nulos == len(df):
            log_warning(f"Todas las fechas en '{columna_fecha}' son nulas", "Transformacion")
            return df

        # Variables derivadas
        df[f'{prefijo}year'] = fechas.dt.year
        df[f'{prefijo}month'] = fechas.dt.month
        df[f'{prefijo}day'] = fechas.dt.day
        df[f'{prefijo}quarter'] = fechas.dt.quarter
        df[f'{prefijo}day_of_week'] = fechas.dt.dayofweek  # 0=Lunes
        df[f'{prefijo}month_name'] = fechas.dt.month_name()
        df[f'{prefijo}is_weekend'] = fechas.dt.dayofweek >= 5
        df[f'{prefijo}day_name'] = fechas.dt.day_name()

        self._registrar_transformacion(
            f"Variables temporales derivadas de '{columna_fecha}': "
            f"year, month, day, quarter, day_of_week, month_name, is_weekend, day_name "
            f"({len(df) - nulos} registros válidos)"
        )

        return df

    # ==================== CATEGORIZACIÓN DE PRECIOS ====================

    def categorizar_precios(
        self,
        df: pd.DataFrame,
        columna_precio: str = 'price_normalized',
        nueva_columna: str = 'price_category',
        bins: Optional[List[float]] = None,
        labels: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Categoriza precios en rangos definidos.

        Args:
            df: DataFrame con columna de precio normalizado.
            columna_precio: Columna con precios numéricos.
            nueva_columna: Nombre de la columna categórica resultante.
            bins: Límites de los rangos (default: cuartiles).
            labels: Etiquetas para cada rango.

        Returns:
            pd.DataFrame: DataFrame con columna de categoría de precio.
        """
        if df.empty or columna_precio not in df.columns:
            log_warning(f"Columna '{columna_precio}' no encontrada para categorizar", "Transformacion")
            return df

        df = df.copy()
        precios_validos = df[columna_precio].dropna()

        if len(precios_validos) == 0:
            log_warning("No hay precios válidos para categorizar", "Transformacion")
            df[nueva_columna] = 'Sin datos'
            return df

        # Definir bins por defecto (cuartiles)
        if bins is None:
            q1, q2, q3 = precios_validos.quantile([0.25, 0.5, 0.75])
            bins = [0, q1, q2, q3, precios_validos.max()]

        if labels is None:
            labels = ['Bajo', 'Medio-Bajo', 'Medio-Alto', 'Alto']

        # Asegurar que bins cubra todo el rango
        bins = [-np.inf] + bins + [np.inf]
        labels_extended = ['Muy Bajo'] + labels + ['Muy Alto']

        df[nueva_columna] = pd.cut(
            df[columna_precio],
            bins=bins,
            labels=labels_extended,
            include_lowest=True
        )

        # Estadísticas por categoría
        conteo = df[nueva_columna].value_counts().sort_index()
        self._registrar_transformacion(
            f"Precios categorizados en '{nueva_columna}': {dict(conteo)}"
        )

        return df

    # ==================== EXPANSIÓN DE CAMPOS ANIDADOS ====================

    def expandir_amenities(
        self,
        df: pd.DataFrame,
        columna_amenities: str = 'amenities',
        prefijo: str = 'amenity_',
        top_n: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Expande el campo amenities (string con formato JSON/lista) a columnas binarias.

        Args:
            df: DataFrame con columna amenities.
            columna_amenities: Nombre de la columna con amenities.
            prefijo: Prefijo para nuevas columnas.
            top_n: Solo crear columnas para los N amenities más frecuentes.

        Returns:
            pd.DataFrame: DataFrame con columnas binarias de amenities.
        """
        if df.empty or columna_amenities not in df.columns:
            return df

        def parse_amenities(valor: Any) -> List[str]:
            """Parsea el string de amenities a lista."""
            # Manejar valores nulos (incluye arrays de numpy/pandas)
            try:
                if pd.isna(valor):
                    return []
            except (ValueError, TypeError):
                # Si pd.isna falla (ej. array), verificar manualmente
                if valor is None:
                    return []
                if isinstance(valor, float) and np.isnan(valor):
                    return []
            if isinstance(valor, (list, tuple, np.ndarray)):
                return [str(v).strip() for v in valor]
            if isinstance(valor, str):
                # Intentar parsear como JSON/lista de Python
                valor = valor.strip()
                if valor.startswith('[') and valor.endswith(']'):
                    try:
                        import ast
                        return [v.strip().strip('"\'') for v in ast.literal_eval(valor)]
                    except Exception:
                        pass
                # Formato simple separado por comas
                return [v.strip().strip('"\'') for v in valor.split(',') if v.strip()]
            return []

        df = df.copy()

        # Parsear amenities
        amenities_parsed = df[columna_amenities].apply(parse_amenities)

        # Obtener todos los amenities únicos
        all_amenities = set()
        for am in amenities_parsed:
            all_amenities.update(am)

        # Filtrar top_n si se especifica
        if top_n and len(all_amenities) > top_n:
            # Contar frecuencias
            from collections import Counter
            freq = Counter()
            for am in amenities_parsed:
                freq.update(am)
            top_amenities = set([a for a, _ in freq.most_common(top_n)])
            all_amenities = top_amenities

        # Crear columnas binarias
        for amenity in sorted(all_amenities):
            col_name = f"{prefijo}{amenity.lower().replace(' ', '_').replace('-', '_')}"
            # Limpiar nombre de columna (solo alfanumérico y _)
            col_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name)
            # Saltar nombres de columna vacíos
            if not col_name:
                continue
            df[col_name] = amenities_parsed.apply(lambda x: 1 if amenity in x else 0)

        self._registrar_transformacion(
            f"Amenities expandidos: {len(all_amenities)} columnas binarias creadas "
            f"(prefijo: '{prefijo}')"
        )

        return df

    def expandir_host_info(
        self,
        df: pd.DataFrame,
        columna_host: str = 'host'
    ) -> pd.DataFrame:
        """
        Expande información anidada del host (diccionario) a columnas planas.

        Args:
            df: DataFrame con columna host (diccionario).
            columna_host: Nombre de la columna con info del host.

        Returns:
            pd.DataFrame: DataFrame con columnas de host expandidas.
        """
        if df.empty or columna_host not in df.columns:
            return df

        df = df.copy()

        def extraer_host(valor: Any) -> Dict:
            """Extrae campos relevantes del host."""
            if pd.isna(valor):
                return {}
            if isinstance(valor, dict):
                return valor
            if isinstance(valor, str):
                try:
                    import ast
                    return ast.literal_eval(valor)
                except Exception:
                    return {}
            return {}

        # Extraer diccionarios
        hosts = df[columna_host].apply(extraer_host)

        # Campos de interés del host
        campos_host = [
            'host_id', 'host_name', 'host_since', 'host_location',
            'host_response_rate', 'host_acceptance_rate', 'host_is_superhost',
            'host_listings_count', 'host_total_listings_count', 'host_verifications'
        ]

        for campo in campos_host:
            col_name = f"host_{campo}" if not campo.startswith('host_') else campo
            df[col_name] = hosts.apply(lambda h: h.get(campo) if isinstance(h, dict) else None)

        # Normalizar tasas (porcentaje a decimal)
        for col_tasa in ['host_response_rate', 'host_acceptance_rate']:
            if col_tasa in df.columns:
                def limpiar_tasa(v):
                    if pd.isna(v):
                        return np.nan
                    if isinstance(v, str) and '%' in v:
                        return float(v.replace('%', '')) / 100
                    if isinstance(v, (int, float)):
                        return float(v) / 100 if float(v) > 1 else float(v)
                    return np.nan
                df[col_tasa] = df[col_tasa].apply(limpiar_tasa)

        # Normalizar host_since a fecha
        if 'host_since' in df.columns:
            df = self.normalizar_fechas(df, ['host_since'])
            df = self.derivar_variables_temporales(df, 'host_since', 'host_')

        self._registrar_transformacion(
            f"Información de host expandida: {len(campos_host)} campos extraídos"
        )

        return df

    # ==================== TRANSFORMACIONES ESPECÍFICAS POR DATASET ====================

    def transformar_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones específicas al dataset Listings.

        Returns:
            pd.DataFrame: Listings transformado y limpio.
        """
        if df.empty:
            return df

        log_info("Iniciando transformación de Listings", "Transformacion")
        df = df.copy()
        inicial = len(df)

        # 1. Normalizar precio
        if 'price' in df.columns:
            df = self.normalizar_precio(df, 'price', 'price_normalized')

        # 2. Categorizar precios
        if 'price_normalized' in df.columns:
            df = self.categorizar_precios(df, 'price_normalized', 'price_category')

        # 3. Expandir host
        if 'host' in df.columns:
            df = self.expandir_host_info(df, 'host')

        # 4. Expandir amenities
        if 'amenities' in df.columns:
            df = self.expandir_amenities(df, 'amenities', 'amenity_', top_n=30)

        # 5. Normalizar fechas relevantes
        fecha_cols = ['last_review', 'first_review', 'host_since']
        fecha_cols = [c for c in fecha_cols if c in df.columns]
        if fecha_cols:
            df = self.normalizar_fechas(df, fecha_cols)
            for fc in fecha_cols:
                df = self.derivar_variables_temporales(df, fc, fc.replace('_', '') + '_')

        # 6. Limpiar nulos en columnas críticas
        criticas = ['price_normalized', 'room_type', 'property_type']
        criticas = [c for c in criticas if c in df.columns]
        if criticas:
            df = self.limpiar_nulos(df, 'Listings', 'eliminar', criticas)

        # 7. Eliminar duplicados por ID
        if 'id' in df.columns:
            df = self.eliminar_duplicados(df, 'Listings', subset=['id'])

        # 8. Eliminar columnas con objetos complejos (listas, dicts) que no se pueden guardar en SQLite
        columnas_complejas = ['amenities', 'host', 'price_quote_raw', 'host_verifications']
        for col in columnas_complejas:
            if col in df.columns:
                # Verificar si contiene objetos complejos
                sample = df[col].dropna().head(1)
                if len(sample) > 0:
                    val = sample.iloc[0]
                    if isinstance(val, (list, dict, tuple)):
                        df = df.drop(columns=[col])
                        self._registrar_transformacion(f"Columna compleja eliminada: '{col}' (tipo: {type(val).__name__})")

        final = len(df)
        self._registrar_transformacion(
            f"Listings transformado: {inicial} -> {final} registros"
        )

        return df

    def transformar_reviews(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones específicas al dataset Reviews.

        Returns:
            pd.DataFrame: Reviews transformado y limpio.
        """
        if df.empty:
            return df

        log_info("Iniciando transformación de Reviews", "Transformacion")
        df = df.copy()
        inicial = len(df)

        # 1. Normalizar fecha
        if 'date' in df.columns:
            df = self.normalizar_fechas(df, ['date'])
            df = self.derivar_variables_temporales(df, 'date', 'review_')

        # 2. Limpiar nulos en columnas críticas
        criticas = ['listing_id', 'reviewer_id', 'date']
        criticas = [c for c in criticas if c in df.columns]
        if criticas:
            df = self.limpiar_nulos(df, 'Reviews', 'eliminar', criticas)

        # 3. Eliminar duplicados
        subset = ['listing_id', 'reviewer_id', 'date']
        subset = [c for c in subset if c in df.columns]
        if subset:
            df = self.eliminar_duplicados(df, 'Reviews', subset=subset)

        final = len(df)
        self._registrar_transformacion(
            f"Reviews transformado: {inicial} -> {final} registros"
        )

        return df

    def transformar_calendar(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones específicas al dataset Calendar.

        Returns:
            pd.DataFrame: Calendar transformado y limpio.
        """
        if df.empty:
            return df

        log_info("Iniciando transformación de Calendar", "Transformacion")
        df = df.copy()
        inicial = len(df)

        # 1. Normalizar precio
        if 'price' in df.columns:
            df = self.normalizar_precio(df, 'price', 'price_normalized')

        # 2. Normalizar fecha
        if 'date' in df.columns:
            df = self.normalizar_fechas(df, ['date'])
            df = self.derivar_variables_temporales(df, 'date', 'cal_')

        # 3. Convertir available a booleano
        if 'available' in df.columns:
            df['available'] = df['available'].map({'t': True, 'f': False, True: True, False: False})

        # 4. Limpiar nulos en columnas críticas
        criticas = ['listing_id', 'date', 'price_normalized']
        criticas = [c for c in criticas if c in df.columns]
        if criticas:
            df = self.limpiar_nulos(df, 'Calendar', 'eliminar', criticas)

        # 5. Eliminar duplicados
        subset = ['listing_id', 'date']
        subset = [c for c in subset if c in df.columns]
        if subset:
            df = self.eliminar_duplicados(df, 'Calendar', subset=subset)

        final = len(df)
        self._registrar_transformacion(
            f"Calendar transformado: {inicial} -> {final} registros"
        )

        return df

    # ==================== PIPELINE COMPLETO ====================

    def transformar_todo(
        self,
        datos: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        Ejecuta el pipeline completo de transformación para todos los datasets.

        Args:
            datos: Diccionario con DataFrames crudos {'Listings': df, 'Reviews': df, 'Calendar': df}.

        Returns:
            Dict[str, pd.DataFrame]: Diccionario con DataFrames transformados.
        """
        log_info("=== INICIANDO PIPELINE COMPLETO DE TRANSFORMACIÓN ===", "Transformacion")

        resultados = {}

        # Transformar cada dataset
        if 'Listings' in datos:
            resultados['Listings'] = self.transformar_listings(datos['Listings'])

        if 'Reviews' in datos:
            resultados['Reviews'] = self.transformar_reviews(datos['Reviews'])

        if 'Calendar' in datos:
            resultados['Calendar'] = self.transformar_calendar(datos['Calendar'])

        # Resumen final
        log_info("=== PIPELINE DE TRANSFORMACIÓN COMPLETADO ===", "Transformacion")
        for nombre, df in resultados.items():
            log_info(f"  {nombre}: {len(df)} registros, {len(df.columns)} columnas", "Transformacion")

        return resultados

    def obtener_estadisticas(self) -> Dict:
        """
        Retorna estadísticas acumuladas de todas las transformaciones.

        Returns:
            Dict: Estadísticas del proceso.
        """
        return self._stats.copy()

    def generar_reporte_transformacion(self) -> str:
        """
        Genera un reporte textual de las transformaciones realizadas.

        Returns:
            str: Reporte formateado.
        """
        lineas = ["=" * 60]
        lineas.append("REPORTE DE TRANSFORMACIONES - ETL AIRBNB")
        lineas.append("=" * 60)

        for nombre in self._stats['registros_iniciales']:
            ini = self._stats['registros_iniciales'].get(nombre, 0)
            fin = self._stats['registros_finales'].get(nombre, 0)
            nulos = self._stats['nulos_eliminados'].get(nombre, 0)
            dup = self._stats['duplicados_eliminados'].get(nombre, 0)
            lineas.append(f"\n{nombre}:")
            lineas.append(f"  Registros iniciales: {ini}")
            lineas.append(f"  Registros finales:   {fin}")
            lineas.append(f"  Nulos tratados:       {nulos}")
            lineas.append(f"  Duplicados eliminados: {dup}")
            lineas.append(f"  Reducción: {((ini-fin)/ini*100):.1f}%" if ini > 0 else "  Reducción: N/A")

        lineas.append("\n" + "-" * 60)
        lineas.append("TRANSFORMACIONES APLICADAS:")
        for i, t in enumerate(self._stats['transformaciones'], 1):
            lineas.append(f"  {i}. {t}")

        return "\n".join(lineas)


if __name__ == "__main__":
    # Prueba rápida con datos sintéticos
    print("=== Prueba de Transformación ===")

    # Crear datos de prueba
    df_listings = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'price': ['$100.00', '$200,50', '$300', 'invalid'],
        'room_type': ['Entire home', 'Private room', 'Shared room', 'Entire home'],
        'last_review': ['2023-01-15', '2023-02-20', None, '2023-03-10'],
        'host': [
            {'host_id': 100, 'host_name': 'Ana', 'host_is_superhost': True},
            {'host_id': 101, 'host_name': 'Carlos', 'host_is_superhost': False},
            {'host_id': 102, 'host_name': 'Maria', 'host_is_superhost': True},
            'invalid'
        ],
        'amenities': [
            '["WiFi", "Kitchen", "Air conditioning"]',
            '["WiFi", "TV"]',
            '["Kitchen"]',
            '[]'
        ]
    })

    df_reviews = pd.DataFrame({
        'listing_id': [1, 1, 2, 3],
        'reviewer_id': [10, 11, 12, 13],
        'date': ['2023-01-20', '2023-02-15', '2023-03-01', '2023-03-15'],
        'comments': ['Great!', 'Good', 'OK', 'Excellent']
    })

    df_calendar = pd.DataFrame({
        'listing_id': [1, 1, 2, 2],
        'date': ['2023-06-01', '2023-06-02', '2023-06-01', '2023-06-02'],
        'price': ['$100', '$120', '$80', '$85'],
        'available': ['t', 'f', 't', 't']
    })

    datos = {'Listings': df_listings, 'Reviews': df_reviews, 'Calendar': df_calendar}

    # Transformar
    tf = Transformacion()
    resultados = tf.transformar_todo(datos)

    # Mostrar resultados
    for nombre, df in resultados.items():
        print(f"\n--- {nombre} ({len(df)} filas, {len(df.columns)} cols) ---")
        print(df.head())
        print(f"Columnas: {list(df.columns)}")

    print("\n" + tf.generar_reporte_transformacion())