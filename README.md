# Workshop Final: Pipeline ETL Orquestado con Airflow

## 1. Objetivo del Proyecto

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) robusto y observable, orquestado con **Apache Airflow** y completamente contenido en **Docker**.

El pipeline sigue un flujo de 3 fases para procesar datos de ventas:
1.  **Fase 1 (Staging):** Limpia, deduplica y valida los datos crudos.
2.  **Fase 2 (Core ETL):** Transforma los datos limpios, los enriquece con datos de negocio (joins) y genera artefactos de salida (tablas de hechos y agregaciones).
3.  **Fase 3 (Reporting):** Genera un análisis final y visualizaciones (gráficos, heatmaps) a partir de los datos procesados.

## 2. Arquitectura de la Solución

* **Orquestación:** Apache Airflow (Imagen `apache/airflow:2.8.1-python3.11`).
* **Entorno:** Docker Compose (con una imagen personalizada construida *en línea*).
* **Lógica ETL:** Python 3.11 (Pandas), modularizado en el directorio `etl/`.
* **Visualización:** Matplotlib & Seaborn (instalados en la imagen de Docker para la Fase 3).
* **Base de Datos (Airflow):** PostgreSQL 15.



---

## 3. Prerrequisitos

Para ejecutar este proyecto, solo necesitas tener instalado el siguiente software en tu máquina host:

1.  **Docker Engine**
2.  **Docker Compose** (usualmente incluido en la instalación de Docker Desktop).
3.  Una **terminal** o línea de comandos (como PowerShell, CMD, Git Bash en Windows, o Terminal en macOS/Linux).

No es necesario instalar Python, Airflow o PostgreSQL localmente; todo se ejecuta dentro de contenedores Docker.

---

## 4. Pasos para el Despliegue (Desde Cero)

Sigue estos pasos en orden para levantar todo el entorno y ejecutar el pipeline.

### Paso 1: Obtener el Proyecto
Clona o descarga este repositorio en tu máquina local.

### Paso 2: Crear el Archivo de Entorno (`.env`)
1.  Busca el archivo `.env.example`.
2.  Crea una copia de este archivo en la misma raíz del proyecto y llámala `.env`.
    * En Windows (PowerShell): `copy .env.example .env`
    * En macOS/Linux (Bash): `cp .env.example .env`

### Paso 3: Generar y Configurar la `FERNET_KEY`
1.  La `FERNET_KEY` es necesaria para que Airflow encripte sus conexiones.
2.  Ejecuta el siguiente comando en tu terminal para generar una clave:
    ```bash
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    ```
3.  Copia la clave generada (es una cadena larga de texto, ej: `a5fBqz...1mfaXw=`).
4.  Abre tu archivo `.env` y pega la clave **después del signo igual** en la línea `AIRFLOW__CORE__FERNET_KEY`, así:
    ```env
    # .env
    AIRFLOW__CORE__FERNET_KEY=a5fBqz9J7iR_jP9vT0x1A_g-Nq-4G1jGgYwJ-1mfaXw=
    ...
    ```

### Paso 4: Construir la Imagen y Levantar los Servicios
1.  **Opcional (Recomendado si ya existían):** Para asegurar un inicio 100% limpio, elimina volúmenes antiguos (esto borrará la base de datos de Airflow si ya existía).
    ```bash
    docker-compose down --volumes
    ```
2.  **¡Comando Principal!** Ejecuta el siguiente comando. Esto construirá la imagen personalizada (instalando `matplotlib`/`seaborn`) y luego iniciará todos los servicios (Postgres, Airflow, etc.).
    ```bash
    docker-compose up -d --build
    ```
    * `--build`: Es **esencial**. Le dice a Docker que construya la imagen desde el `docker-compose.yml` (instalando las librerías).
    * `-d`: Ejecuta los contenedores en modo "detached" (en segundo plano).

### Paso 5: Acceder a la Interfaz de Airflow
1.  Espera unos 30-60 segundos para que todos los servicios se inicien.
2.  Abre tu navegador web y ve a: **`http://localhost:8080`**
3.  Inicia sesión con las credenciales por defecto (creadas por el contenedor `init`):
    * **Usuario:** `admin`
    * **Contraseña:** `admin`

### Paso 6: Ejecutar el Pipeline (DAG)
1.  En la página principal de Airflow, busca el DAG llamado **`workshop_final_etl_v2`**.
2.  **Actívalo** moviendo el interruptor "Off" a "On".
3.  Para lanzarlo manualmente, haz clic en el botón de "Play" (▶️) a la derecha del nombre del DAG y selecciona "Trigger DAG".
4.  Puedes hacer clic en el nombre del DAG para ver su progreso en la vista de "Graph" o "Grid". Espera a que todas las 5 tareas se completen (se pondrán de color verde oscuro).

---

## 5. Verificación de Salidas

Una vez que el DAG se complete exitosamente, puedes verificar los artefactos generados directamente en las carpetas de tu proyecto:

* **Datos Procesados (CSV):**
    * `./data/output/fact_transactions.csv`
    * `./data/output/agg_daily.csv`

* **Reportes y Gráficos (PNG):**
    * `./data/output/visualizations/client_marketing_region.png`
    * `./data/output/visualizations/product_country_category_heatmap.png`
    * `./data/output/visualizations/temporal_daily_sales.png`
    * `...y todos los demás gráficos generados.`

---

## 6. Observabilidad y Troubleshooting

### ¿Dónde encontrar los Logs?

* **Opción 1 (Recomendada - UI de Airflow):**
    1.  Haz clic en el DAG `workshop_final_etl_v2`.
    2.  Ve a la vista "Grid".
    3.  Haz clic en uno de los cuadrados (una tarea) que se haya ejecutado.
    4.  En el menú emergente, haz clic en "Log". Aquí verás todos los `print()` y `logging.info()` de los scripts de Python (ej. los `[Insight Cliente]...` del `EDA_FINAL.py`).

* **Opción 2 (Archivos Físicos):**
    * Todos los logs de ejecución de tareas se guardan en la carpeta `./logs` de tu proyecto, mapeados desde el contenedor.

### Errores Comunes y Soluciones

* **Error: `Permission denied` al intentar escribir en `./logs`**
    * **Síntoma:** Los contenedores `scheduler` o `webserver` fallan al iniciarse, o el contenedor `init` muestra un error de permisos.
    * **Causa:** El usuario de Airflow (UID 50000) dentro del contenedor no tiene permisos de escritura sobre la carpeta `./logs` en tu máquina host.
    * **Solución:** El contenedor `init` está diseñado para arreglar esto automáticamente al ejecutarse como `root` y aplicar `chmod 777`. Si el problema persiste, detén todo (`docker-compose down`) y vuelve a intentarlo.

* **Error: `Invalid login` (admin/admin no funciona)**
    * **Síntoma:** No puedes iniciar sesión en `http://localhost:8080`.
    * **Causa:** El contenedor `init` falló *antes* de poder crear el usuario. Esto suele pasar si el `airflow db init` falló o si el contenedor se detuvo prematuramente.
    * **Solución:**
        1.  Verifica los logs del contenedor `init`: `docker-compose logs airflow-init`.
        2.  La solución más robusta es borrar la base de datos (que puede estar en un estado corrupto) y empezar de nuevo:
            ```bash
            docker-compose down --volumes 
            docker-compose up -d --build
            ```

* **Error: `Broken DAG` (ImportError: No module named 'matplotlib')**
    * **Síntoma:** El DAG `workshop_final_etl_v2` aparece con un error rojo en la UI.
    * **Causa:** El script del DAG o uno de sus módulos (como `EDA_FINAL.py`) importa una librería que no está instalada en el contenedor de Airflow.
    * **Solución:** Este error *no debería* ocurrir con el `docker-compose.yml` actual, ya que instala `matplotlib` y `seaborn`. Si añades una **nueva** librería (ej. `scikit-learn`), debes añadirla al `dockerfile_inline` en `docker-compose.yml`:
        ```yaml
        # ...
        dockerfile_inline: |
          FROM apache/airflow:2.8.1-python3.11
          RUN pip install --no-cache-dir matplotlib seaborn scikit-learn # <--- Añadir aquí
        # ...
        ```
        Y luego **re-construir** la imagen: `docker-compose up -d --build`.

---

## 7. Estructura del Proyecto

. 
├── dags/ 
   │ 
   └── workshop_etl.py # DAG principal de Airflow que define el flujo de 5 tareas. 
├── data/ 
   │ 
   ├── raw/ # Datos fuente (CSV). 
   │ 
   ├── staging/ # Datos limpios intermedios (Parquet) - Creados por la Fase 1. 
   │ 
   └── output/ # Artefactos finales - Creados por la Fase 2 y 3. 
      │ 
      ├── visualizations/ # Gráficos (PNG) 
      │ 
      └── *.csv # Tablas de datos (CSV)
├── etl/ 
   │ 
   ├── EDA.py # FASE 1: Limpieza (Raw -> Staging). 
   │ 
   ├── extract.py # FASE 2: Extracción (Staging -> Memoria). 
   │ 
   ├── transform.py # FASE 2: Transformación (Joins, Agregaciones). 
   │ 
   ├── load.py # FASE 2: Carga (Memoria -> Output CSV). 
   │ 
   └── EDA_FINAL.py # FASE 3: Reporte y Visualización (Output -> Gráficos). 
├── logs/ # Logs de Airflow (mapeados al host). 
├── .env # Variables de entorno (¡SECRETO!) 
├── .env.example # Plantilla para .env 
├── docker-compose.yml # Definición de servicios (TODO-EN-UNO). 
└── README.md # Esta documentación.

## 8. Lógica del Pipeline (Detalle de Tareas)

El DAG `workshop_final_etl_v2` consta de 5 tareas secuenciales:

1.  **`initial_cleaning_and_eda`**
    * **Script:** `etl/EDA.py`
    * **Acción:** Lee los CSV de `data/raw/`. Aplica la limpieza profunda (nulos, duplicados, outliers, formatos). Guarda los DataFrames limpios como `.parquet` en `data/staging/`.

2.  **`extract_cleaned_data`**
    * **Script:** `etl/extract.py`
    * **Acción:** Lee los archivos `.parquet` de `data/staging/`. También lee `country_region.csv` (manejando el caso especial de "NA"), y pasa todos los DataFrames a la siguiente tarea vía XCom.

3.  **`transform_business_logic`**
    * **Script:** `etl/transform.py`
    * **Acción:** Aplica la lógica de negocio. Realiza los `joins` entre transacciones, clientes, productos y regiones. Genera dos DataFrames finales: `fact_transactions` (tabla de hechos detallada) y `agg_daily` (agregación diaria).

4.  **`load_final_csv_outputs`**
    * **Script:** `etl/load.py`
    * **Acción:** Toma los dos DataFrames de la tarea anterior y los escribe como CSV (`fact_transactions.csv`, `agg_daily.csv`) en el directorio `data/output/`.

5.  **`final_analysis_and_reporting`**
    * **Script:** `etl/EDA_FINAL.py`
    * **Acción:** Lee los CSV finales de `data/output/`. Realiza agregaciones (por día de la semana, país/categoría) y genera todas las visualizaciones (`.png`), guardándolas en `data/output/visualizations/`. También imprime los `[Insights]` clave en los logs de Airflow.