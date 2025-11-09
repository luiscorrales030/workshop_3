import pandas as pd
import logging

def transform_data(dfs_dict: dict) -> dict:
    """
    Orquesta el proceso de transformación con datos limpios.
    Realiza joins, deduplicación final y agregaciones.

    Args:
        dfs_dict (dict): Diccionario de DFs limpios de extract.py.

    Returns:
        dict: Un diccionario con los DataFrames finales:
              {"fact_transactions": df_fact, "agg_daily": df_agg}
    """
    logging.info("Iniciando proceso de Transformación (Lógica de Negocio)...")
    
    df_txns = dfs_dict["transactions"]
    df_customers = dfs_dict["customers"]
    df_products = dfs_dict["products"]
    df_country_region = dfs_dict["country_region"]

    # --- 1. Integración y Reglas (Joins) ---
    logging.info("Realizando Joins (Productos, Clientes, Región)...")
    
    # Join con Products
    df_final = df_txns.merge(
        df_products, 
        on='product_id', 
        how='left'
    )
    
    # Join con Customers
    df_final = df_final.merge(
        df_customers, 
        on='customer_id', 
        how='left'
    )
    
    # Join con Country Region
    df_final = df_final.merge(
        df_country_region, 
        on='country', 
        how='left'
    )
    # Llenar regiones desconocidas que no hicieron match
    df_final['region'] = df_final['region'].fillna('Unknown')
    
    # --- 2. Creación de la Tabla de Hechos (Fact Table) ---
    # Seleccionar columnas para la tabla de hechos final
    fact_columns = [
        'txn_id', 'ts', 'date', 'status',
        'customer_id', 'country', 'region', 'marketing_source', 'signup_ts',
        'product_id', 'category', 'price_list',
        'amount', 'currency', 'amount_usd'
    ]
    # Asegurar que solo seleccionamos columnas que existen
    fact_columns = [col for col in fact_columns if col in df_final.columns]
    df_fact_transactions = df_final[fact_columns]
    
    logging.info(f"Tabla de Hechos 'fact_transactions' creada con {len(df_fact_transactions)} filas.")

    # --- 3. Agregaciones y Salidas  ---
    logging.info("Generando agregaciones diarias (region/país/categoría)...")
    
    df_agg_daily = df_final.groupby(
        ['date', 'region', 'country', 'category']
    ).agg(
        n_transactions=('txn_id', 'count'),
        total_usd=('amount_usd', 'sum')
    ).reset_index()
    
    logging.info(f"Tabla 'agg_daily' creada con {len(df_agg_daily)} filas.")

    # --- 4. Retorno de Artefactos ---
    return {
        "fact_transactions": df_fact_transactions,
        "agg_daily": df_agg_daily
    }