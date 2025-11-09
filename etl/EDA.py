"""
Módulo de Pre-procesamiento y Análisis Exploratorio de Datos (EDA) Inicial.

Lee datos de 'data/raw/', realiza una limpieza profunda, análisis de calidad
y guarda los datos limpios en 'data/staging/' para el pipeline ETL.
"""

import pandas as pd
import numpy as np
import os
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constantes
RAW_PATH = '/opt/airflow/data/raw'
STAGING_PATH = '/opt/airflow/data/staging'
STATUS_MAP = {
    "paid": "paid", "Paid": "paid", "PAID": "paid",
    "refunded": "refunded", "Refunded": "refunded",
    "cancelled": "cancelled"
}

def clean_amount(series: pd.Series) -> pd.Series:
    """Limpia la columna 'amount' eliminando símbolos y ajustando comas decimales."""
    if pd.api.types.is_string_dtype(series):
        return pd.to_numeric(
            series.astype(str)
                .str.replace(r'[$,€]', '', regex=True)
                .str.replace(',', '.', regex=False),
            errors='coerce'
        )
    return pd.to_numeric(series, errors='coerce')

def analyze_and_clean_data(raw_data_path: str, staging_data_path: str):
    """
    Función principal que orquesta la carga, limpieza profunda y análisis inicial.
    Guarda los dataframes limpios en el directorio staging.
    """
    logging.info("Iniciando Fase 1: Limpieza y EDA Inicial...")
    
    # Asegurar que el directorio staging exista
    os.makedirs(staging_data_path, exist_ok=True)

    # --- 1. Cargar Datos ---
    try:
        df_customers = pd.read_csv(os.path.join(raw_data_path, 'customers.csv'))
        df_products = pd.read_csv(os.path.join(raw_data_path, 'products.csv'))
        df_txns = pd.read_csv(os.path.join(raw_data_path, 'transactions.csv'))
        
        # Crear y/o cargar tasas de cambio
        exchange_rates_path = os.path.join(raw_data_path, 'exchange_rates.csv')
        if not os.path.exists(exchange_rates_path):
            rates_data = {"USD": 1.0, "EUR": 1.08, "MXN": 0.058, "BRL": 0.18}
            pd.DataFrame(rates_data.items(), columns=['currency', 'rate_to_usd']).to_csv(exchange_rates_path, index=False)
            logging.info(f"Archivo 'exchange_rates.csv' creado en {exchange_rates_path}")
        
        df_rates = pd.read_csv(exchange_rates_path)
        
    except FileNotFoundError as e:
        logging.error(f"Error fatal: Archivo no encontrado. {e}")
        raise
    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
        raise

    # --- 2. Limpieza y Análisis: TRANSACTIONS ---
    logging.info(f"[EDA Txns] Filas iniciales: {len(df_txns)}")

    # Calidad: Duplicados por txn_id
    dupes = df_txns.duplicated(subset=['txn_id']).sum()
    logging.info(f"[EDA Txns] Transacciones duplicadas (txn_id): {dupes}")
    df_txns.drop_duplicates(subset=['txn_id'], keep='first', inplace=True)
    logging.info(f"[EDA Txns] Filas post-deduplicación: {len(df_txns)}")

    # Calidad: Nulos críticos
    critical_cols = ['customer_id', 'product_id', 'ts', 'amount']
    nulos_antes = df_txns[critical_cols].isna().sum()
    logging.info(f"[EDA Txns] Nulos críticos (antes): \n{nulos_antes}")
    df_txns.dropna(subset=critical_cols, inplace=True)
    logging.info(f"[EDA Txns] Filas post-eliminación nulos críticos: {len(df_txns)}")

    # Limpieza: Estandarización de 'status'
    df_txns['status'] = df_txns['status'].map(STATUS_MAP).fillna('unknown')
    logging.info(f"[EDA Txns] Conteo de 'status' normalizado: \n{df_txns['status'].value_counts(dropna=False)}")

    # Limpieza: Parseo robusto de 'ts' (formatos mixtos)
    df_txns['ts'] = pd.to_datetime(df_txns['ts'], errors='coerce')
    invalid_dates = df_txns['ts'].isna().sum()
    logging.info(f"[EDA Txns] Fechas inválidas (eliminadas): {invalid_dates}")
    df_txns.dropna(subset=['ts'], inplace=True)

    # Limpieza: Normalización de 'amount' (símbolos/comas)
    df_txns['amount'] = clean_amount(df_txns['amount'])
    invalid_amounts = df_txns['amount'].isna().sum()
    logging.info(f"[EDA Txns] Montos inválidos (eliminados): {invalid_amounts}")
    df_txns.dropna(subset=['amount'], inplace=True)

    # Calidad: Outliers (análisis rápido de 'amount')
    logging.info(f"[EDA Txns] Análisis de Outliers ('amount'): \n{df_txns['amount'].describe([.01, .25, .5, .75, .99])}")
    # Definir outliers (ej. > 99.9%) - aquí solo informamos
    q_high = df_txns["amount"].quantile(0.999)
    outliers_count = (df_txns["amount"] > q_high).sum()
    logging.info(f"[EDA Txns] Conteo de outliers (amount > {q_high:.2f}): {outliers_count}")

    # Monedas: Normalización a USD
    df_txns = df_txns.merge(df_rates, on='currency', how='left')
    # Si falta la tasa (ej. 'USD'), asumimos 1.0
    df_txns['rate_to_usd'] = df_txns['rate_to_usd'].fillna(1.0)
    df_txns['amount_usd'] = df_txns['amount'] * df_txns['rate_to_usd']
    logging.info("[EDA Txns] Columna 'amount_usd' creada.")
    logging.info(f"[EDA Txns] Distribución de 'amount_usd': \n{df_txns['amount_usd'].describe()}")

    # Temporal: Picos/caídas (análisis simple)
    df_txns['date'] = df_txns['ts'].dt.date
    daily_sales = df_txns.groupby('date')['amount_usd'].sum()
    logging.info(f"[EDA Txns] Análisis Temporal (Ventas Diarias USD): \n{daily_sales.describe()}")


    # --- 3. Limpieza y Análisis: CUSTOMERS ---
    logging.info(f"[EDA Cust] Filas iniciales: {len(df_customers)}")
    
    # Calidad: Nulos en 'country'
    country_nulos_reales = df_customers['country'].isna().sum()
    logging.info(f"[EDA Cust] Nulos reales en 'country': {country_nulos_reales}")

    # Paso 2: Reemplazamos únicamente los verdaderos nulos, sin tocar valores válidos como "NA"
    df_customers['country'] = df_customers['country'].fillna('Unknown')
    
    # Calidad: 'signup_ts'
    df_customers['signup_ts'] = pd.to_datetime(df_customers['signup_ts'], errors='coerce')
    
    # Clientes: Cohortes por 'signup_ts' (análisis mensual)
    df_customers['signup_month'] = df_customers['signup_ts'].dt.to_period('M')
    cohort_analysis = df_customers.groupby('signup_month').size()
    logging.info(f"[EDA Cust] Análisis de Cohortes (nuevos clientes/mes): \n{cohort_analysis.tail()}")


    # --- 4. Limpieza y Análisis: PRODUCTS ---
    logging.info(f"[EDA Prod] Filas iniciales: {len(df_products)}")
    df_products['price_list'] = pd.to_numeric(df_products['price_list'], errors='coerce')


    # --- 5. Guardar Datos Limpios en Staging ---
    # Usamos Parquet para la etapa intermedia por eficiencia
    df_txns.to_parquet(os.path.join(staging_data_path, 'txns_cleaned.parquet'), index=False)
    df_customers.to_parquet(os.path.join(staging_data_path, 'customers_cleaned.parquet'), index=False)
    df_products.to_parquet(os.path.join(staging_data_path, 'products_cleaned.parquet'), index=False)
    
    logging.info(f"Fase 1 completada. Datos limpios guardados en {staging_data_path}")

if __name__ == "__main__":
    # Esto permite ejecutar el script de forma independiente si es necesario
    analyze_and_clean_data(RAW_PATH, STAGING_PATH)