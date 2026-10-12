"""
Módulo de Extracción - ETL Airbnb Bogotá

Clase Extraccion para conectar a MongoDB y extraer colecciones a DataFrames de pandas
con registro de logs.
"""

import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from typing import Dict, Optional, List
import sys
import os

# Agregar src al path para importar logger
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from logger import log_info, log_warning, log_error, get_logger


class Extraccion:
    """
    Clase para la extracción de datos desde MongoDB.

    Permite:
    - Establecer conexión con la base de datos MongoDB local
    - Consultar colecciones (Listings, Reviews, Calendar)
    - Cargar datos en DataFrames de pandas
    - Registrar logs de conexión y cantidad de registros extraídos
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        database_name: str = "airbnb_bogota",
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_source: str = "admin"
    ) -> None:
        """
        Inicializa la configuración de conexión a MongoDB.

        Args:
            host: Host de MongoDB (default: localhost)
            port: Puerto de MongoDB (default: 27017)
            database_name: Nombre de la base de datos (default: airbnb_bogota)
            username: Usuario para autenticación (opcional)
            password: Contraseña para autenticación (opcional)
            auth_source: Base de datos de autenticación (default: admin)
        """
        self.host = host
        self.port = port
        self.database_name = database_name
        self.username = username
        self.password = password
        self.auth_source = auth_source

        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None
        self._connected = False

        # Logger específico para este módulo
        self.logger = get_logger()

    def conectar(self) -> bool:
        """
        Establece la conexión con MongoDB.

        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        try:
            # Construir URI de conexión
            if self.username and self.password:
                uri = (
                    f"mongodb://{self.username}:{self.password}@"
                    f"{self.host}:{self.port}/{self.database_name}"
                    f"?authSource={self.auth_source}"
                )
            else:
                uri = f"mongodb://{self.host}:{self.port}/"

            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)

            # Verificar conexión con ping
            self._client.admin.command('ping')

            self._db = self._client[self.database_name]
            self._connected = True

            log_info(
                f"Conexión exitosa a MongoDB: {self.host}:{self.port}/{self.database_name}",
                "Extraccion"
            )
            return True

        except Exception as e:
            log_error(f"Error al conectar a MongoDB: {str(e)}", "Extraccion")
            self._connected = False
            return False

    def desconectar(self) -> None:
        """Cierra la conexión con MongoDB."""
        if self._client:
            self._client.close()
            self._connected = False
            log_info("Conexión a MongoDB cerrada", "Extraccion")

    def verificar_conexion(self) -> bool:
        """
        Verifica si la conexión está activa.

        Returns:
            bool: True si está conectado, False en caso contrario.
        """
        if not self._connected or not self._client:
            return False
        try:
            self._client.admin.command('ping')
            return True
        except Exception:
            self._connected = False
            return False

    def listar_colecciones(self) -> List[str]:
        """
        Lista todas las colecciones disponibles en la base de datos.

        Returns:
            List[str]: Nombres de las colecciones.
        """
        if not self.verificar_conexion():
            log_warning("No hay conexión activa a MongoDB", "Extraccion")
            return []

        try:
            colecciones = self._db.list_collection_names()
            log_info(f"Colecciones encontradas: {colecciones}", "Extraccion")
            return colecciones
        except Exception as e:
            log_error(f"Error al listar colecciones: {str(e)}", "Extraccion")
            return []

    def extraer_coleccion(
        self,
        nombre_coleccion: str,
        filtro: Optional[Dict] = None,
        limite: Optional[int] = None,
        proyeccion: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Extrae una colección de MongoDB y la carga en un DataFrame de pandas.

        Args:
            nombre_coleccion: Nombre de la colección a extraer.
            filtro: Diccionario con filtro de consulta MongoDB (opcional).
            limite: Número máximo de documentos a extraer (opcional).
            proyeccion: Campos a incluir/excluir (opcional).

        Returns:
            pd.DataFrame: DataFrame con los datos de la colección.
        """
        if not self.verificar_conexion():
            log_error("No hay conexión activa a MongoDB", "Extraccion")
            return pd.DataFrame()

        try:
            collection: Collection = self._db[nombre_coleccion]

            # Construir consulta
            query = filtro or {}
            cursor = collection.find(query, proyeccion)

            if limite:
                cursor = cursor.limit(limite)

            # Convertir a lista y luego a DataFrame
            datos = list(cursor)

            if not datos:
                log_warning(f"La colección '{nombre_coleccion}' está vacía", "Extraccion")
                return pd.DataFrame()

            df = pd.DataFrame(datos)

            # Eliminar campo _id de MongoDB si existe (no es útil para análisis)
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])

            log_info(
                f"Extracción exitosa: '{nombre_coleccion}' - "
                f"{len(df)} registros, {len(df.columns)} columnas",
                "Extraccion"
            )

            return df

        except Exception as e:
            log_error(f"Error al extraer colección '{nombre_coleccion}': {str(e)}", "Extraccion")
            return pd.DataFrame()

    def extraer_todas(
        self,
        colecciones: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Extrae múltiples colecciones de una sola vez.

        Args:
            colecciones: Lista de nombres de colecciones a extraer.
                        Si es None, extrae las colecciones estándar: Listings, Reviews, Calendar.

        Returns:
            Dict[str, pd.DataFrame]: Diccionario con nombre_coleccion -> DataFrame.
        """
        if colecciones is None:
            colecciones = ["Listings", "Reviews", "Calendar"]

        resultados = {}

        for coleccion in colecciones:
            df = self.extraer_coleccion(coleccion)
            resultados[coleccion] = df

        # Resumen final
        total_registros = sum(len(df) for df in resultados.values())
        log_info(
            f"Extracción completa: {len(resultados)} colecciones, "
            f"{total_registros} registros totales",
            "Extraccion"
        )

        return resultados

    def obtener_info_coleccion(self, nombre_coleccion: str) -> Dict:
        """
        Obtiene información básica de una colección (conteo, índices, etc.).

        Args:
            nombre_coleccion: Nombre de la colección.

        Returns:
            Dict: Información de la colección.
        """
        if not self.verificar_conexion():
            return {}

        try:
            collection: Collection = self._db[nombre_coleccion]
            count = collection.count_documents({})
            indexes = collection.index_information()

            info = {
                "nombre": nombre_coleccion,
                "documentos": count,
                "indices": list(indexes.keys())
            }

            log_info(f"Info colección '{nombre_coleccion}': {count} documentos", "Extraccion")
            return info

        except Exception as e:
            log_error(f"Error al obtener info de '{nombre_coleccion}': {str(e)}", "Extraccion")
            return {}

    def __enter__(self) -> 'Extraccion':
        """Soporte para context manager (with)."""
        self.conectar()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Cierra conexión al salir del context manager."""
        self.desconectar()


# Función de conveniencia para extracción rápida
def extraer_datos_airbnb(
    host: str = "localhost",
    port: int = 27017,
    database_name: str = "airbnb_bogota",
    colecciones: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Función de conveniencia para extraer datos de Airbnb en una línea.

    Args:
        host: Host de MongoDB.
        port: Puerto de MongoDB.
        database_name: Nombre de la base de datos.
        colecciones: Lista de colecciones a extraer.

    Returns:
        Dict[str, pd.DataFrame]: Diccionario con los DataFrames extraídos.
    """
    with Extraccion(host, port, database_name) as extractor:
        return extractor.extraer_todas(colecciones)


if __name__ == "__main__":
    # Prueba rápida del módulo
    print("=== Prueba de Extracción ===")

    with Extraccion() as ext:
        if ext.verificar_conexion():
            print("[OK] Conexion exitosa")
            colecciones = ext.listar_colecciones()
            print(f"Colecciones: {colecciones}")

            for col in ["Listings", "Reviews", "Calendar"]:
                info = ext.obtener_info_coleccion(col)
                if info:
                    print(f"  {col}: {info['documentos']} docs")

            # Extraer una muestra
            dfs = ext.extraer_todas(["Listings", "Reviews", "Calendar"])
            for nombre, df in dfs.items():
                if not df.empty:
                    print(f"\n{nombre}: {df.shape}")
                    print(f"  Columnas: {list(df.columns[:10])}...")
        else:
            print("[ERROR] No se pudo conectar a MongoDB")
            print("  Verifique que MongoDB esté corriendo en localhost:27017")
            print("  Y que la base de datos 'airbnb_bogota' exista con las colecciones")