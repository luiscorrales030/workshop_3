# Workshop Final — ETL Orquestado con Airflow

**Objetivo**: Construir un pipeline ETL robusto, profesional y replicable utilizando Apache Airflow, Docker y Python (Pandas) para procesar datos de ventas, limpiarlos, transformarlos y cargarlos en un data mart unificado.

## 1. Arquitectura de la Solución

Este proyecto utiliza **Docker Compose** para orquestar un entorno completo de Apache Airflow.

* **Orquestación**: Apache Airflow (Imagen `apache/airflow:2.8.1-python3.11`).
* **Backend de Metadatos**: PostgreSQL (Imagen `postgres:15`).
* **Lógica ETL**: Python 3.11 con Pandas, desacoplada en el directorio `/etl`.
* **Almacenamiento de Datos**: Los datos crudos (`/data/raw`) y procesados (`/data/processed`) se manejan a través de volúmenes de Docker.


## 2. Cómo Desplegar la Solución

Sigue estos pasos para levantar el pipeline.

**Requisitos Previos:**
* Docker
* Docker Compose

**Pasos:**

1.  **Clonar el Repositorio (si aplica):**
    ```bash
    git clone [URL-DEL-REPO]
    cd [NOMBRE-DEL-REPO]
    ```

2.  **Configurar el Entorno:**
    El archivo `.env.example` se proporciona como plantilla. Renómbrelo (o cópielo) a `.env`.
    ```bash
    cp .env.example .env
    ```
    *Nota: El archivo `.env` ya contiene una `FERNET_KEY` de ejemplo y la configuración de la base de datos. Para un entorno de producción, debe generar su propia Fernet Key.*

3.  **Construir e Iniciar los Contenedores:**
    Este comando inicializará la base de datos, el programador (scheduler) y el servidor web (webserver) de Airflow.
    ```bash
    docker-compose up -d
    ```

4.  **Inicializar Airflow (Primera Vez):**
    El servicio `init` se ejecutará automáticamente (definido en el `docker-compose.yml`) para inicializar la base de datos y crear un usuario administrador.

5.  **Acceder a la Interfaz de Airflow:**
    * Abre tu navegador y ve a: `http://localhost:8080`
    * **Usuario:** `admin`
    * **Contraseña:** `admin`

6.  **Ejecutar el DAG:**
    * En la interfaz de Airflow, busca el DAG llamado `workshop_final_etl`.
    * Actívalo (moviendo el interruptor a "On").
    * Haz clic en el botón de "Play" (Trigger DAG) para iniciar una ejecución manual.

7.  **Verificar el Resultado:**
    Una vez que el DAG se complete exitosamente, encontrarás el data mart final en la siguiente ruta (dentro de tu proyecto):
    `./data/processed/sales_data_mart.parquet`

## 3. Estructura del Proyecto

.├── dags/ │ └── workshop_etl.py # El DAG de Airflow que orquesta el pipeline. ├── data/ │ ├── raw/ # Datos de origen (CSV). │ │ ├── customers.csv │ │ ├── products.csv │ │ ├── transactions.csv │ │ └── exchange_rates.csv # Creado por el ETL si no existe. │ └── processed/ # Datos limpios y transformados. │ └── .gitkeep ├── etl/ # Lógica de negocio (Python/Pandas). │ ├── extract.py # Script para leer datos (E). │ ├── transform.py # Script para limpiar y enriquecer (T). │ └── load.py # Script para guardar datos (L). ├── logs/ # Logs de Airflow. ├── plugins/ # Plugins (vacío por ahora). ├── .env # Variables de entorno (PostgreSQL, Fernet Key). ├── .env.example # Plantilla de variables de entorno. ├── docker-compose.yml # Definición de servicios (Airflow, Postgres). └── README.md # Esta documentación.


## 4. Lógica del Pipeline ETL

El pipeline se divide en tres etapas claras, orquestadas por el DAG `workshop_etl.py`.

### Tarea 1: Extract (extracción)
* **Script:** `etl/extract.py`
* **Lógica:**
    1.  Verifica si `data/raw/exchange_rates.csv` existe. Si no, lo crea con las tasas definidas en el README (USD, EUR, MXN, BRL).
    2.  Lee `customers.csv`, `products.csv`, `transactions.csv` y `exchange_rates.csv` y los carga en diccionarios de DataFrames de Pandas.
    3.  Pasa este diccionario a la siguiente tarea vía XCom.

### Tarea 2: Transform (transformación)
* **Script:** `etl/transform.py`
* **Lógica:** Esta es la etapa más compleja, basada en los hallazgos del EDA.
    1.  **Limpieza de `transactions`:**
        * **Nulos:** Elimina filas donde `customer_id`, `product_id` o `ts` son nulos.
        * **Amount (Monto):** Elimina símbolos (`$`, `€`) y reemplaza comas decimales (`,`) por puntos (`.`). Convierte la columna a tipo `float`. Las filas que no se pueden convertir se eliminan.
        * **Status (Estado):** Normaliza los estados usando el `STATUS_MAP` (ej. "Paid" -> "paid").
        * **Timestamp (ts):** Convierte la columna a `datetime`. Las filas con fechas inválidas se eliminan.
    2.  **Limpieza de `customers`:**
        * **Nulos:** Rellena los `country` nulos con el valor "Unknown".
    3.  **Conversión de Moneda:**
        * Realiza un `join` de `transactions` con `exchange_rates` usando la columna `currency`.
        * Crea una nueva columna `amount_usd` multiplicando `amount * rate_to_usd`.
    4.  **Enriquecimiento (Joins):**
        * Une `transactions` (limpio) con `products` (limpio) usando `product_id`.
        * Une el resultado con `customers` (limpio) usando `customer_id`.
    5.  **Selección Final:** Selecciona un conjunto final de columnas para crear el data mart unificado.

### Tarea 3: Load (carga)
* **Script:** `etl/load.py`
* **Lógica:**
    1.  Recibe el DataFrame transformado final de la tarea anterior.
    2.  Guarda el DataFrame en `data/processed/sales_data_mart.parquet` usando el motor `pyarrow` para una compresión y rendimiento óptimos.

## 5. Resumen del Análisis Exploratorio de Datos (EDA)

El análisis inicial de los datos crudos (ver `EDA.md` o la sección en el entregable principal) reveló los siguientes problemas clave que este ETL resuelve:

* **`transactions.csv` (Datos Críticos):**
    * **Monto Sucio:** La columna `amount` contenía símbolos de moneda y comas decimales, requiriendo limpieza (regex).
    * **Estado Inconsistente:** La columna `status` tenía múltiples valores (ej. "paid", "Paid", "PAID"), requiriendo normalización.
    * **Nulos Críticos:** Se encontraron nulos en `customer_id`, `product_id` y `ts`. Estas filas fueron **eliminadas** por ser inutilizables.
* **`customers.csv` (Datos Menores):**
    * Se encontraron nulos en `country`, que fueron **imputados** con "Unknown" para no perder registros de clientes.
* **`exchange_rates.csv` (Datos Faltantes):**
    * El archivo no existía y fue **creado** por el script de extracción según los requisitos.