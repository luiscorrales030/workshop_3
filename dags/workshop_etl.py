import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# --- Configuración del DAG ---

# Añadir el directorio raíz del proyecto al path de Python
# Esto permite que Airflow importe los módulos del directorio 'etl'
# Usamos /opt/airflow/ porque es donde mapeamos el volumen en docker-compose.yml
PROJECT_HOME = '/opt/airflow'
if PROJECT_HOME not in sys.path:
    sys.path.append(PROJECT_HOME)

# Importar la lógica ETL
try:
    from etl import extract, transform, load
except ImportError as e:
    logging.error(f"Error importando módulos ETL: {e}")
    # Si no puede importar, fallará al cargar el DAG, lo cual es deseable.
    raise

# --- Definición de Rutas ---
# Estas rutas son DENTRO del contenedor de Airflow
RAW_DATA_PATH = '/opt/airflow/data/raw'
PROCESSED_DATA_PATH = '/opt/airflow/data/processed'

# --- Argumentos por Defecto del DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # Fecha de inicio fija
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # Reintentar 2 veces en caso de fallo
    'retry_delay': timedelta(minutes=2), # Esperar 2 minutos entre reintentos
}

# --- Definición del DAG ---
with DAG(
    dag_id='workshop_final_etl',
    default_args=default_args,
    description='Pipeline ETL completo para el workshop final.',
    schedule_interval='@daily', # Ejecución diaria
    catchup=False, # No ejecutar tareas pasadas al iniciar
    tags=['workshop', 'etl', 'data-engineering'],
) as dag:

    # --- Tarea 1: Extracción ---
    def extract_task():
        """
        Wrapper de PythonOperator para la función de extracción.
        Retorna el diccionario de DataFrames vía XCom.
        """
        logging.info("Iniciando Tarea de Extracción (E)...")
        dfs_dict = extract.extract_data(raw_data_path=RAW_DATA_PATH)
        # Nota: Airflow serializará y pasará esto vía XCom automáticamente.
        # Para dataframes grandes, se recomienda usar almacenamiento externo (ej. S3).
        # Para este workshop, XCom es suficiente.
        logging.info("Tarea de Extracción (E) completada.")
        return dfs_dict

    # --- Tarea 2: Transformación ---
    def transform_task(ti):
        """
        Wrapper para la función de transformación.
        Recibe el diccionario de DataFrames desde la Tarea 1 vía XCom.
        """
        logging.info("Iniciando Tarea de Transformación (T)...")
        # Obtener el resultado (dfs_dict) de la tarea anterior
        dfs_dict = ti.xcom_pull(task_ids='extract_data')
        if dfs_dict is None:
            raise ValueError("No se recibieron datos de la tarea de extracción (XCom pull retornó None).")
            
        df_transformed = transform.transform_data(dfs_dict=dfs_dict)
        logging.info("Tarea de Transformación (T) completada.")
        return df_transformed

    # --- Tarea 3: Carga ---
    def load_task(ti):
        """
        Wrapper para la función de carga.
        Recibe el DataFrame transformado desde la Tarea 2 vía XCom.
        """
        logging.info("Iniciando Tarea de Carga (L)...")
        df_to_load = ti.xcom_pull(task_ids='transform_data')
        if df_to_load is None:
            raise ValueError("No se recibieron datos de la tarea de transformación (XCom pull retornó None).")

        load.load_data(df=df_to_load, processed_data_path=PROCESSED_DATA_PATH)
        logging.info("Tarea de Carga (L) completada.")


    # --- Definición de Tareas (PythonOperator) ---
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
    )

    # --- Definición de Dependencias ---
    # La extracción debe ocurrir primero, luego la transformación, y finalmente la carga.
    task_extract >> task_transform >> task_load