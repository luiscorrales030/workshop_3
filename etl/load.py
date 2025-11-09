import pandas as pd
import os
import logging

def load_data(dataframes_dict: dict, output_data_path: str):
    """
    Guarda los DataFrames finales (fact y agg) en formato CSV
    en el directorio 'data/output/'.

    Args:
        dataframes_dict (dict): Diccionario de DFs de transform.py.
        output_data_path (str): Ruta al directorio de salida.
    """
    
    # Asegurarse de que el directorio de salida exista
    os.makedirs(output_data_path, exist_ok=True)
    
    try:
        # 1. Guardar Tabla de Hechos
        df_fact = dataframes_dict.get("fact_transactions")
        if df_fact is not None:
            output_path_fact = os.path.join(output_data_path, "fact_transactions.csv")
            logging.info(f"Iniciando carga de 'fact_transactions.csv' en: {output_path_fact}")
            df_fact.to_csv(output_path_fact, index=False, encoding='utf-8')
            logging.info(f"Carga de 'fact_transactions.csv' completada. {len(df_fact)} filas guardadas.")
        else:
            logging.warning("No se encontró 'fact_transactions' para guardar.")

        # 2. Guardar Tabla Agregada
        df_agg = dataframes_dict.get("agg_daily")
        if df_agg is not None:
            output_path_agg = os.path.join(output_data_path, "agg_daily.csv")
            logging.info(f"Iniciando carga de 'agg_daily.csv' en: {output_path_agg}")
            df_agg.to_csv(output_path_agg, index=False, encoding='utf-8')
            logging.info(f"Carga de 'agg_daily.csv' completada. {len(df_agg)} filas guardadas.")
        else:
            logging.warning("No se encontró 'agg_daily' para guardar.")

    except IOError as e:
        logging.error(f"Error de E/S al escribir el archivo CSV: {e}")
        raise
    except Exception as e:
        logging.error(f"Un error inesperado ocurrió durante la carga: {e}")
        raise