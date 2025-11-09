"""
Módulo de Análisis Final y Generación de Reportes.

Lee los artefactos finales de 'data/output/', responde a preguntas
de negocio clave y genera visualizaciones y conclusiones.

"""

import pandas as pd
import os
import logging
import matplotlib
matplotlib.use('Agg') # Usar backend no interactivo para Airflow
import matplotlib.pyplot as plt
import seaborn as sns

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constantes de Rutas
OUTPUT_PATH = '/opt/airflow/data/output'
VIZ_PATH = '/opt/airflow/data/output/visualizations'

def final_analysis_and_reporting(output_data_path: str):
    """
    Función principal que orquesta el análisis final y la generación
    de visualizaciones y conclusiones.
    """
    logging.info("Iniciando Fase 3: Análisis Final y Reporte...")
    
    # Asegurar que el directorio de visualizaciones exista
    os.makedirs(VIZ_PATH, exist_ok=True)
    
    # --- 1. Cargar Datos Finales ---
    try:
        df_fact = pd.read_csv(os.path.join(output_data_path, 'fact_transactions.csv'))
        df_agg = pd.read_csv(os.path.join(output_data_path, 'agg_daily.csv'))
        
        # Convertir columnas de fecha
        df_fact['ts'] = pd.to_datetime(df_fact['ts'], errors='coerce')
        df_fact['date'] = pd.to_datetime(df_fact['date'], errors='coerce')
        df_agg['date'] = pd.to_datetime(df_agg['date'], errors='coerce')
        
        # Eliminar cualquier nulo en columnas clave que pudiera persistir
        df_fact.dropna(subset=['amount_usd', 'category', 'region', 'marketing_source', 'country'], inplace=True)

    except FileNotFoundError as e:
        logging.error(f"Error fatal: Archivos de salida no encontrados. {e}")
        raise
    except Exception as e:
        logging.error(f"Error al cargar datos de 'output': {e}")
        raise

    logging.info("Datos finales cargados. Calculando agregaciones...")

    # --- 2. Cálculo Centralizado de Agregaciones ---
    
    # Para Conclusiones y Gráfico de Productos
    agg_category = df_fact.groupby('category')['amount_usd'].agg(['mean', 'sum', 'count']).sort_values(by='sum', ascending=False)
    
    # Para Conclusiones y Gráfico de Clientes
    agg_marketing_region = df_fact.groupby(['region', 'marketing_source'])['amount_usd'].sum().unstack(fill_value=0)

    # Para Conclusiones y Gráfico Temporal (Diario)
    daily_sales = df_agg.groupby('date')['total_usd'].sum().sort_index() # .sort_index() es CRÍTICO
    
    # Para Análisis Temporal (Semanal)
    df_fact['weekday'] = df_fact['ts'].dt.day_name()
    agg_weekly = df_fact.groupby('weekday')['amount_usd'].sum().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ])
    
    # Para Análisis de Productos (Combinaciones Anómalas País x Categoría)
    agg_country_category = df_fact.groupby(['country', 'category']).size().unstack(fill_value=0)


    # --- 3. Conclusiones Accionables (Impresas en Log) ---
    logging.info("--- Conclusiones Accionables ---")
    
    try:
        # Conclusión 1: Basada en Productos
        top_category = agg_category.index[0]
        logging.info(f"[Insight Producto] La categoría '{top_category}' es la que más ingresos genera.")
        logging.info(f"\n{agg_category.to_string()}") 

        # Conclusión 2: Basada en Clientes/Región
        top_region = agg_marketing_region.sum(axis=1).idxmax()
        logging.info(f"[Insight Cliente] La región '{top_region}' es el mercado más grande.")
        logging.info(f"\n{agg_marketing_region.to_string()}") 
        
        # Conclusión 3: Basada en Temporalidad (Diaria)
        peak_day = daily_sales.idxmax()
        logging.info(f"[Insight Temporal] El día pico de ventas fue {peak_day.date()}.")

        # Conclusión 4: Basada en Patrones Semanales
        peak_weekday = agg_weekly.idxmax()
        logging.info(f"[Insight Temporal] El día de la semana con más ventas es {peak_weekday}.")
        logging.info(f"\n{agg_weekly.to_string()}")

        # Conclusión 5: Basada en Combinaciones
        logging.info(f"[Insight Productos] Matriz País x Categoría generada con {agg_country_category.shape[0]} países y {agg_country_category.shape[1]} categorías.")

    except Exception as e:
        logging.error(f"Error al generar conclusiones de texto: {e}")

    # --- 4. Generación de Visualizaciones ---
    logging.info("Generando visualizaciones mejoradas...")

    # Gráfico 1: Distribución de Montos (Quantile 95%)
    plt.figure(figsize=(10, 6))
    q95 = df_fact['amount_usd'].quantile(0.95)
    sns.histplot(df_fact[df_fact['amount_usd'] < q95]['amount_usd'], bins=50, kde=True)
    plt.title(f'Distribución de Montos de Transacción (USD) - Hasta Quantile 95 ({q95:.2f})')
    plt.xlabel('Monto (USD)')
    plt.ylabel('Frecuencia')
    plt.savefig(os.path.join(VIZ_PATH, 'dist_amount_usd_q95.png'))
    logging.info(f"Visualización 'dist_amount_usd_q95.png' guardada.")
    plt.close()

    # Gráfico 2: Distribución de Montos (Escala Logarítmica)
    plt.figure(figsize=(10, 6))
    sns.histplot(df_fact['amount_usd'], bins=100, kde=False, log_scale=True)
    plt.title('Distribución de TODOS los Montos de Transacción (Escala Logarítmica)')
    plt.xlabel('Monto (USD) - Escala Log')
    plt.ylabel('Frecuencia')
    plt.savefig(os.path.join(VIZ_PATH, 'dist_amount_usd_log.png'))
    logging.info(f"Visualización 'dist_amount_usd_log.png' guardada.")
    plt.close()

    # Gráfico 3: Ventas Diarias (Temporal)
    plt.figure(figsize=(14, 7))
    daily_sales.plot(title='Ventas Diarias Totales (USD)')
    plt.xlabel('Fecha')
    plt.ylabel('Total Ventas (USD)')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_PATH, 'temporal_daily_sales.png'))
    logging.info(f"Visualización 'temporal_daily_sales.png' guardada.")
    plt.close()

    # Gráfico 4: Dispersión por Categoría (Boxplot)
    plt.figure(figsize=(12, 8))
    sns.boxplot(data=df_fact, x='category', y='amount_usd', showfliers=True) # Mostrar outliers
    plt.title('Dispersión del Monto de Venta por Categoría (Escala Log)')
    plt.xticks(rotation=45)
    plt.yscale('log') # Usar escala logarítmica para manejar outliers
    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_PATH, 'product_boxplot_category.png'))
    logging.info(f"Visualización 'product_boxplot_category.png' guardada.")
    plt.close()

    # Gráfico 5: Ventas por Marketing y Región (Barras)
    agg_marketing_region_plot = agg_marketing_region / 1_000_000 # Escalar a millones
    agg_marketing_region_plot.plot(kind='bar', stacked=True, figsize=(12, 7))
    plt.title('Ventas (USD) por Fuente de Marketing y Región')
    plt.ylabel('Total Ventas (Millones USD)') # Etiqueta corregida
    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_PATH, 'client_marketing_region.png'))
    logging.info(f"Visualización 'client_marketing_region.png' guardada.")
    plt.close()
    
    
    # Gráfico 6 (NUEVO): Ventas por Día de la Semana
    agg_weekly_plot = agg_weekly / 1_000_000
    plt.figure(figsize=(10, 6))
    agg_weekly_plot.plot(kind='bar', title='Ventas Totales (Millones USD) por Día de la Semana')
    plt.xlabel('Día de la Semana')
    plt.ylabel('Total Ventas (Millones USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_PATH, 'temporal_weekly_sales.png'))
    logging.info(f"Visualización 'temporal_weekly_sales.png' guardada.")
    plt.close()
    
    # Gráfico 7 (NUEVO): Heatmap de País x Categoría
    # (Solo mostramos las Top 20 países para legibilidad)
    try:
        top_20_countries = df_fact['country'].value_counts().index[:20]
        agg_country_category_top20 = agg_country_category.loc[agg_country_category.index.isin(top_20_countries)]
        
        plt.figure(figsize=(12, 10))
        sns.heatmap(agg_country_category_top20, cmap='viridis', annot=True, fmt='d', linewidths=.5)
        plt.title('Heatmap de Transacciones: Top 20 Países vs. Categoría')
        plt.xlabel('Categoría')
        plt.ylabel('País')
        plt.tight_layout()
        plt.savefig(os.path.join(VIZ_PATH, 'product_country_category_heatmap.png'))
        logging.info(f"Visualización 'product_country_category_heatmap.png' guardada.")
    except Exception as e:
        logging.error(f"No se pudo generar el heatmap de país/categoría: {e}")
    plt.close()

    logging.info("Fase 3: Análisis Final completado.")

if __name__ == "__main__":
    final_analysis_and_reporting(OUTPUT_PATH)