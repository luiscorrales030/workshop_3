import pandas as pd
import os
import logging

# Configurar logging profesional
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_exchange_rates(filepath: str, data: dict):
    """
    Crea el archivo de tasas de cambio si no existe.
    """
    if not os.path.exists(filepath):
        try:
            logging.info(f"Creando archivo de tasas de cambio en: {filepath}")
            df = pd.DataFrame(data.items(), columns=['currency', 'rate_to_usd'])
            df.to_csv(filepath, index=False)
        except IOError as e:
            logging.error(f"No se pudo escribir el archivo de tasas de cambio: {e}")
            raise
    else:
        logging.info("El archivo de tasas de cambio ya existe, no se sobrescribe.")

def extract_data(raw_data_path: str) -> dict:
    """
    Carga todos los datasets crudos desde CSV a DataFrames de pandas.
    Crea el archivo de tasas de cambio si es necesario.

    Args:
        raw_data_path (str): Ruta al directorio de datos crudos.

    Returns:
        dict: Un diccionario de DataFrames (ej. {"customers": df, "products": df, ...})
    """
    logging.info(f"Iniciando extracción de datos desde: {raw_data_path}")
    
    # Definir rutas de archivos
    customers_path = os.path.join(raw_data_path, 'customers.csv')
    products_path = os.path.join(raw_data_path, 'products.csv')
    transactions_path = os.path.join(raw_data_path, 'transactions.csv')
    exchange_rates_path = os.path.join(raw_data_path, 'exchange_rates.csv')

    # 1. Asegurar que existe el archivo de tasas de cambio (requisito del README)
    rates_data = {
        "USD": 1.0,
        "EUR": 1.08,
        "MXN": 0.058,
        "BRL": 0.18
    }
    create_exchange_rates(exchange_rates_path, rates_data)

    # 2. Cargar todos los datasets
    try:
        dfs = {
            "customers": pd.read_csv(customers_path),
            "products": pd.read_csv(products_path),
            "transactions": pd.read_csv(transactions_path),
            "exchange_rates": pd.read_csv(exchange_rates_path)
        }
        logging.info("Extracción de todos los archivos completada exitosamente.")
        return dfs
    except FileNotFoundError as e:
        logging.error(f"Error en la extracción: Archivo no encontrado. {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logging.error(f"Error en la extracción: Archivo vacío o corrupto. {e}")
        raise