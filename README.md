# ETL Airbnb Bogotá

Un proyecto para tomar los datos públicos de Airbnb en Bogotá (alojamientos, calendario y reseñas), limpiarlos y dejarlos listos para analizar. Es el **Taller evaluativo 2** de Inteligencia de Negocios (ITM).

## ¿Qué hace?

Imagina una cadena de tres pasos:

1. **Extrae** los datos desde una base MongoDB que tienes en tu computador.
2. **Transforma**: arregla precios (`$150,000.00` → `150000`), fechas, quita duplicados y vacíos, y crea columnas útiles (mes, trimestre, fin de semana, categoría de precio, amenidades).
3. **Carga** el resultado en una base **SQLite** y en archivos **Excel (XLSX)**.

En cada paso el programa va escribiendo un diario (el *log*) en la carpeta `logs/`, para que puedas ver qué hizo y cuántos registros procesó.

## Qué necesitas antes de empezar

- **Python 3.9 o superior**
- **MongoDB** (Community Server) y, opcional, **MongoDB Compass** para ver los datos
- Los tres archivos del taller: `listings.csv.gz`, `reviews.csv.gz` y `calendar.csv.gz`

## Paso 1: Cargar los datos en MongoDB

Esto se hace una sola vez. Con **Compass** es lo más fácil:

1. Abre Compass y conéctate a `localhost:27017`.
2. Crea la base de datos **`airbnb_bogota`** con la colección **`Listings`**.
3. Dentro de esa base crea también las colecciones **`Reviews`** y **`Calendar`**.
4. En cada colección usa **Add Data → Import File**, elige el archivo que corresponde (formato CSV) y espera a que termine.
5. Comprueba que cada colección tenga documentos.

> Los nombres deben quedar exactamente así (con mayúscula inicial): `Listings`, `Reviews`, `Calendar`.

¿Prefieres la terminal? Descomprime los `.gz` y ejecuta:

```bash
mongoimport --db airbnb_bogota --collection Listings --type csv --headerline --file listings.csv
mongoimport --db airbnb_bogota --collection Reviews  --type csv --headerline --file reviews.csv
mongoimport --db airbnb_bogota --collection Calendar --type csv --headerline --file calendar.csv
```

## Paso 2: Preparar Python

Desde la carpeta del proyecto:

```bash
# Crear el entorno virtual
python -m venv venv

# Activarlo
venv\Scripts\activate          # Windows
source venv/bin/activate       # Mac / Linux

# Instalar las librerías
pip install -r requirements.txt
```

## Paso 3: Ejecutar el ETL

Con MongoDB encendido:

```bash
python src/etl_main.py
```

Eso es todo. Al terminar verás `[OK] ETL COMPLETADO EXITOSAMENTE` y tendrás:

| Qué | Dónde |
|---|---|
| Base de datos SQLite | `data/airbnb_etl.db` |
| Archivos Excel | `data/xlsx_export/` |
| Diario de la ejecución | `logs/log_AAAAMMDD_HHMM.txt` |

### Ejemplo de lo que verás

```
INFO | [ETL_Main] INICIANDO PROCESO ETL COMPLETO - AIRBNB BOGOTÁ
INFO | [Extraccion] Conexión exitosa a MongoDB: localhost:27017/airbnb_bogota
INFO | [Extraccion] Extracción exitosa: 'Listings' - 2500 registros, 39 columnas
INFO | [Transformacion] Precio normalizado: 'price' -> 'price_normalized' (2500 válidos, 0 nulos)
INFO | [Transformacion] Reviews: Eliminados 24 duplicados (18610 -> 18586)
INFO | [Carga] Cargada tabla 'reviews': 18586 registros (DataFrame tenía 18586)
INFO | [ETL_Main] PROCESO ETL COMPLETADO EXITOSAMENTE
```

*(Las cifras dependen de tus datos.)*

Si tu MongoDB no está en la configuración por defecto, puedes indicarlo:

```bash
python src/etl_main.py --mongo-host localhost --mongo-port 27017 --mongo-db airbnb_bogota
```

## Paso 4 (opcional): Ver el análisis exploratorio

```bash
jupyter notebook notebooks/exploracion_airbnb.ipynb
```

Ejecuta todas las celdas para ver gráficas y hallazgos. Hazlo **antes** del ETL si quieres seguir el orden natural: primero entender los datos, luego limpiarlos.



## ¿Cómo está organizado?

```
etl_airbnb/
├── src/            El ETL: extraccion.py, transformacion.py, carga.py, logger.py y etl_main.py
├── notebooks/      El análisis exploratorio (EDA)
├── informe/        Generador del informe PDF
├── logs/           Aquí se guardan los diarios de cada ejecución
├── data/           Aquí quedan SQLite y Excel (se generan solos)
├── docs/           Guía técnica detallada (por si necesitas más detalle)
├── README.md
└── requirements.txt
```

## Integrantes y responsabilidades


| Integrante | Responsabilidades |
|---|---|
| Santiago Lopez Vallejo  |
| Sebastian Garcia Velez  |
|Ambos encargados de el desarrollo de todo el proyecto |

## Si algo falla

| Síntoma | Qué hacer |
|---|---|
| "No se pudo conectar a MongoDB" | Asegúrate de que MongoDB esté encendido (en Windows: `net start MongoDB`). |
| "Ninguna de las colecciones existe" | Revisa que la base se llame `airbnb_bogota` y las colecciones `Listings`, `Reviews`, `Calendar`. |
| `ModuleNotFoundError` | Activa el entorno virtual y vuelve a ejecutar `pip install -r requirements.txt`. |
| Se queda sin memoria o tarda mucho | El calendario es muy grande (millones de filas). Cierra otros programas; para el informe puedes usar `--max-calendar 1000000`. |

Para más detalle de cada clase y método, mira `docs/GUIA_TECNICA.md`.
