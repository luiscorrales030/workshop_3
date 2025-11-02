import pandas as pd
import os
import logging

def load_data(df: pd.DataFrame, processed_data_path: str, filename: str = "sales_data_mart.parquet"):
    """
    Guarda el DataFrame transformado en formato Parquet.

    Args:
        df (pd.DataFrame): El DataFrame limpio y transformado.
        processed_data_path (str): Ruta al directorio de datos procesados.
        filename (str, optional): Nombre del archivo de salida.
    """
    
    # Asegurarse de que el directorio de salida exista
    os.makedirs(processed_data_path, exist_ok=True)
    
    output_path = os.path.join(processed_data_path, filename)
    
    try:
        logging.info(f"Iniciando carga de datos en: {output_path}")
        df.to_parquet(output_path, index=False, engine='pyarrow')
        logging.info(f"Carga completada exitosamente. {len(df)} filas guardadas.")
    except IOError as e:
        logging.error(f"Error al escribir el archivo Parquet: {e}")
        raise
    except Exception as e:
        logging.error(f"Un error inesperado ocurrió durante la carga: {e}")
        raise