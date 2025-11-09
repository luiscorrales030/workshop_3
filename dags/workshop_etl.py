import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# --- Configuración del DAG ---
PROJECT_HOME = '/opt/airflow'
if PROJECT_HOME not in sys.path:
    sys.path.append(PROJECT_HOME)

# Importar la lógica ETL y los nuevos módulos de EDA
try:
    from etl import extract, transform, load
    from etl import EDA as InitialEDA
    from etl import EDA_FINAL as FinalEDA
except ImportError as e:
    logging.error(f"Error importando módulos ETL o EDA: {e}")
    raise

# --- Definición de Rutas (Dentro del Contenedor) ---
RAW_DATA_PATH = '/opt/airflow/data/raw'
STAGING_DATA_PATH = '/opt/airflow/data/staging'
OUTPUT_DATA_PATH = '/opt/airflow/data/output'

# --- Argumentos por Defecto del DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # 1 reintento
    'retry_delay': timedelta(minutes=1),
}

# --- Definición del DAG ---
with DAG(
    dag_id='workshop_final_etl_v2',
    default_args=default_args,
    description='Pipeline ETL (V2) con fases de Pre-EDA, ETL Core y Post-EDA.',
    schedule_interval='@daily',
    catchup=False,
    tags=['workshop', 'etl', 'v2', 'csv'],
) as dag:

    # --- Tarea 1: Limpieza y EDA Inicial ---
    def run_initial_eda_task():
        """
        Ejecuta el script de limpieza profunda y análisis inicial.
        Lee de 'raw' y escribe en 'staging'.
        """
        logging.info("Iniciando Tarea 1: run_initial_eda_task")
        InitialEDA.analyze_and_clean_data(
            raw_data_path=RAW_DATA_PATH, 
            staging_data_path=STAGING_DATA_PATH
        )
        logging.info("Tarea 1 completada.")

    # --- Tarea 2: Extracción (ETL Core) ---
    def extract_task():
        """
        Extrae datos limpios de 'staging' y datos de referencia de 'raw'.
        Pasa los DFs vía XCom.
        """
        logging.info("Iniciando Tarea 2: extract_task")
        dfs_dict = extract.extract_data(
            staging_data_path=STAGING_DATA_PATH,
            raw_data_path=RAW_DATA_PATH
        )
        logging.info("Tarea 2 completada.")
        return dfs_dict

    # --- Tarea 3: Transformación (ETL Core) ---
    def transform_task(ti):
        """
        Recibe DFs limpios de XCom, aplica lógica de negocio
        (joins, agregaciones) y pasa los DFs finales vía XCom.
        """
        logging.info("Iniciando Tarea 3: transform_task")
        dfs_dict = ti.xcom_pull(task_ids='extract_cleaned_data')
        if dfs_dict is None:
            raise ValueError("No se recibieron datos de la tarea de extracción (XCom).")
            
        final_dfs_dict = transform.transform_data(dfs_dict=dfs_dict)
        logging.info("Tarea 3 completada.")
        return final_dfs_dict # Contiene 'fact_transactions' y 'agg_daily'

    # --- Tarea 4: Carga (ETL Core) ---
    def load_task(ti):
        """
        Recibe los DFs finales ('fact' y 'agg') y los
        guarda como CSV en 'data/output/'.
        """
        logging.info("Iniciando Tarea 4: load_task")
        final_dfs_dict = ti.xcom_pull(task_ids='transform_business_logic')
        if final_dfs_dict is None:
            raise ValueError("No se recibieron datos de la tarea de transformación (XCom).")

        load.load_data(
            dataframes_dict=final_dfs_dict, 
            output_data_path=OUTPUT_DATA_PATH
        )
        logging.info("Tarea 4 completada. Salidas CSV generadas.")

    # --- Tarea 5: Análisis Final y Reporte ---
    def run_final_analysis_task():
        """
        Ejecuta el script de análisis final.
        Lee de 'data/output/' y genera visualizaciones y conclusiones.
        """
        logging.info("Iniciando Tarea 5: run_final_analysis_task")
        try:
            FinalEDA.final_analysis_and_reporting(
                output_data_path=OUTPUT_DATA_PATH
            )
        except ImportError as e:
            logging.warning(f"Error de importación en Análisis Final (ignorado): {e}")
            logging.warning("El análisis final falló, probablemente 'matplotlib' o 'seaborn' no están instalados en la imagen de Airflow.")
        except Exception as e:
            logging.error(f"Error en Tarea 5 (Análisis Final): {e}")
            raise
        logging.info("Tarea 5 completada.")


    # --- Definición de Tareas (Operadores) ---
    task_initial_eda = PythonOperator(
        task_id='initial_cleaning_and_eda',
        python_callable=run_initial_eda_task,
    )

    task_extract = PythonOperator(
        task_id='extract_cleaned_data',
        python_callable=extract_task,
    )

    task_transform = PythonOperator(
        task_id='transform_business_logic',
        python_callable=transform_task,
    )

    task_load = PythonOperator(
        task_id='load_final_csv_outputs',
        python_callable=load_task,
    )
    
    task_final_analysis = PythonOperator(
        task_id='final_analysis_and_reporting',
        python_callable=run_final_analysis_task,
    )

    # --- Definición de Dependencias (El flujo completo) ---
    task_initial_eda >> task_extract >> task_transform >> task_load >> task_final_analysis