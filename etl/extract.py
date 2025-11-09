import pandas as pd
import os
import logging
import numpy as np

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(staging_data_path: str, raw_data_path: str) -> dict:
    """
    Carga los datos limpios de la etapa 'staging' y los datos de
    referencia necesarios (country_region) de 'raw'.

    Args:
        staging_data_path (str): Ruta al directorio de datos 'staging'.
        raw_data_path (str): Ruta al directorio de datos 'raw'.

    Returns:
        dict: Un diccionario de DataFrames (limpios).
    """
    logging.info(f"Iniciando extracción de datos limpios desde: {staging_data_path}")
    
    # Definir rutas de archivos
    txns_path = os.path.join(staging_data_path, 'txns_cleaned.parquet')
    customers_path = os.path.join(staging_data_path, 'customers_cleaned.parquet')
    products_path = os.path.join(staging_data_path, 'products_cleaned.parquet')
    country_region_path = os.path.join(raw_data_path, 'country_region.csv')
    
    try:
        dfs = {
            "transactions": pd.read_parquet(txns_path),
            "customers": pd.read_parquet(customers_path),
            "products": pd.read_parquet(products_path),
        }

        logging.info("Cargando 'country_region.csv' con na_filter=False para leer 'NA' como string.")
        
        # 1. Leer el CSV sin filtro de nulos (na_filter=False)
        #    Esto lee todo como string, incluyendo "NA", "", "NULL", etc.
        df_cr = pd.read_csv(
            country_region_path,
            na_filter=False,  # No detectar nulos automáticamente
            dtype=str         # Tratar todas las columnas como string
        )
        
        # 2. Aplicar el mapeo 'NA' -> 'NORTHAMERICA' para evitar que el dropna reemplace por error la region NA, y otros errores relacionados a esto
        logging.info("Aplicando mapeo solicitado: 'NA' -> 'NORTHAMERICA' en 'region'")
        df_cr['region'] = df_cr['region'].replace(
            to_replace='NA', 
            value='NORTHAMERICA'
        )
        
        # 3. Ahora, reemplazar strings vacíos ('') u otros nulos no deseados con un NaN real
        #    (ya que na_filter=False también los leyó como strings)
        df_cr.replace(['', 'NULL', 'N/A', 'n/a', 'NaN'], np.nan, inplace=True)
        
        # 4. Llenar cualquier NaN real (celdas vacías, etc.) con 'Unknown'
        df_cr['region'] = df_cr['region'].fillna('Unknown')
        
        dfs["country_region"] = df_cr

        logging.info("Extracción de datos de staging completada exitosamente.")
        return dfs
    
    except FileNotFoundError as e:
        logging.error(f"Error en la extracción: Archivo no encontrado. {e}")
        raise
    except Exception as e:
        logging.error(f"Error en la extracción: {e}")
        raise