import pandas as pd
import numpy as np
import logging

# Mapeo de estado (provisto en el archivo original)
STATUS_MAP = {
    "paid": "paid", "Paid": "paid", "PAID": "paid",
    "refunded": "refunded", "Refunded": "refunded",
    "cancelled": "cancelled"
}

def _clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia el DataFrame de clientes."""
    logging.info("Iniciando limpieza de 'customers'...")
    df_clean = df.copy()
    
    # Imputar valores nulos en 'country' (según EDA)
    df_clean['country'] = df_clean['country'].fillna('Unknown')
    
    # Convertir 'signup_ts' a datetime
    df_clean['signup_ts'] = pd.to_datetime(df_clean['signup_ts'], errors='coerce')
    
    logging.info(f"'customers' limpiado. Filas nulas en 'signup_ts' (si las hay): {df_clean['signup_ts'].isna().sum()}")
    return df_clean

def _clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia el DataFrame de productos."""
    logging.info("Iniciando limpieza de 'products'...")
    df_clean = df.copy()
    
    # Asegurar que price_list es numérico
    df_clean['price_list'] = pd.to_numeric(df_clean['price_list'], errors='coerce')
    return df_clean

def _clean_transactions(df: pd.DataFrame, rates_df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y transforma el DataFrame de transacciones."""
    logging.info("Iniciando limpieza de 'transactions'...")
    df_clean = df.copy()
    initial_rows = len(df_clean)

    # 1. Dropear filas con nulos críticos (según EDA)
    df_clean.dropna(subset=['customer_id', 'product_id', 'ts'], inplace=True)
    rows_after_na_drop = len(df_clean)
    logging.info(f"Filas eliminadas por nulos críticos: {initial_rows - rows_after_na_drop}")

    # 2. Limpiar columna 'amount' (según EDA)
    # Eliminar símbolos '$' y '€', luego reemplazar ',' por '.'
    df_clean['amount_cleaned'] = df_clean['amount'].astype(str) \
                                        .str.replace(r'[$,€]', '', regex=True) \
                                        .str.replace(',', '.', regex=False)
    df_clean['amount'] = pd.to_numeric(df_clean['amount_cleaned'], errors='coerce')
    
    # Dropear filas donde 'amount' no pudo ser convertido
    rows_before_amount_drop = len(df_clean)
    df_clean.dropna(subset=['amount'], inplace=True)
    logging.info(f"Filas eliminadas por 'amount' no convertible: {rows_before_amount_drop - len(df_clean)}")

    # 3. Normalizar 'status' (usando el MAP)
    df_clean['status'] = df_clean['status'].map(STATUS_MAP)
    # Rellenar cualquier estado no mapeado como 'unknown'
    df_clean['status'] = df_clean['status'].fillna('unknown')

    # 4. Convertir 'ts' a datetime
    df_clean['ts'] = pd.to_datetime(df_clean['ts'], errors='coerce')
    
    # Dropear filas donde 'ts' no pudo ser convertido
    rows_before_ts_drop = len(df_clean)
    df_clean.dropna(subset=['ts'], inplace=True)
    logging.info(f"Filas eliminadas por 'ts' no convertible: {rows_before_ts_drop - len(df_clean)}")

    # 5. Convertir moneda a USD
    df_clean = df_clean.merge(rates_df, on='currency', how='left')
    
    # Si alguna moneda no tiene tasa, se le asignará NaN. La llenamos con 1.0 (asumiendo USD si falta)
    df_clean['rate_to_usd'] = df_clean['rate_to_usd'].fillna(1.0) 
    df_clean['amount_usd'] = df_clean['amount'] * df_clean['rate_to_usd']
    
    logging.info(f"Limpieza de 'transactions' completa. Filas restantes: {len(df_clean)} de {initial_rows}")
    
    return df_clean.drop(columns=['amount_cleaned', 'rate_to_usd']) # Eliminar columnas auxiliares


def transform_data(dfs_dict: dict) -> pd.DataFrame:
    """
    Orquesta todo el proceso de transformación y enriquecimiento.

    Args:
        dfs_dict (dict): El diccionario de DataFrames crudos de extract.py.

    Returns:
        pd.DataFrame: El DataFrame final, limpio y enriquecido.
    """
    logging.info("Iniciando proceso de transformación...")
    
    # 1. Limpiar cada DataFrame individualmente
    df_customers_clean = _clean_customers(dfs_dict["customers"])
    df_products_clean = _clean_products(dfs_dict["products"])
    df_transactions_clean = _clean_transactions(dfs_dict["transactions"], dfs_dict["exchange_rates"])
    
    # 2. Enriquecer: Unir (Join) los DataFrames
    logging.info("Iniciando enriquecimiento de datos (Joins)...")
    
    # Unir transacciones con productos
    df_final = df_transactions_clean.merge(
        df_products_clean, 
        on='product_id', 
        how='left',
        suffixes=('_txn', '_prod') # Sufijos por si hay columnas duplicadas
    )
    
    # Unir resultado con clientes
    df_final = df_final.merge(
        df_customers_clean, 
        on='customer_id', 
        how='left',
        suffixes=('', '_cust')
    )
    
    # 3. Selección final de columnas (creando el "Data Mart")
    columnas_finales = [
        'txn_id', 
        'ts', 
        'status',
        'customer_id', 
        'country', 
        'marketing_source', 
        'signup_ts',
        'product_id', 
        'category', 
        'price_list', # Precio de lista original del producto
        'amount',     # Monto original en moneda local
        'currency',   # Moneda local
        'amount_usd'  # Monto convertido a USD
    ]
    
    # Asegurarnos de que solo existan las columnas que están en el dataframe
    columnas_existentes = [col for col in columnas_finales if col in df_final.columns]
    df_final = df_final[columnas_existentes]
    
    logging.info(f"Transformación y enriquecimiento completados. DataFrame final con {len(df_final)} filas.")
    
    return df_final