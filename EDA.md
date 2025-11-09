# Análisis Exploratorio de Datos (EDA) y Hallazgos del Pipeline

Este documento detalla el proceso de análisis y limpieza realizado en dos fases.

1.  **EDA Inicial (`etl/EDA.py`)**: Un pre-procesamiento que limpia los datos crudos (`data/raw`) y los prepara en una zona intermedia (`data/staging`).
2.  **EDA Final (`etl/EDA_FINAL.py`)**: Un post-procesamiento que analiza los artefactos finales (`data/output`), genera visualizaciones y extrae conclusiones.

---

## 1. Calidad de Datos (Fase 1: `EDA.py`)

La calidad de los datos crudos fue el principal desafío, requiriendo una limpieza robusta en `EDA.py`. Los logs de `initial_cleaning_and_eda` revelan las siguientes métricas:

### Nulos
Se identificaron valores nulos en múltiples archivos:
* **`transactions.csv`**: Se encontraron nulos en columnas críticas. El log muestra:
    ```
    [EDA Txns] Nulos críticos (antes): 
    customer_id    110
    product_id     110
    ts             110
    amount         110
    ```
    Dado que estas filas son inutilizables para el análisis, **fueron eliminadas 110 filas**.
* **`customers.csv`**: Se encontraron nulos en la columna `country`. El log muestra:
    ```
    [EDA Cust] Nulos reales en 'country': 105
    ```
    Para no perder clientes, estos 105 valores fueron **imputados** con el string `"Unknown"`.

### Duplicados por `txn_id`
El dataset `transactions.csv` contenía transacciones duplicadas.
* **Proceso**: Se utilizó `drop_duplicates(subset=['txn_id'], keep='first')` para asegurar la unicidad.
* **Hallazgo**: Los logs confirman la eliminación de duplicados:
    ```
    [EDA Txns] Filas iniciales: 22341
    [EDA Txns] Transacciones duplicadas (txn_id): 10
    [EDA Txns] Filas post-deduplicación: 22331
    ```

### Fechas Inválidas
La columna `ts` (timestamp) en `transactions.csv` contenía formatos mixtos y valores corruptos.
* **Proceso**: Se utilizó `pd.to_datetime(..., errors='coerce')`. Las filas que no pudieron ser parseadas (resultando en `NaT`) fueron eliminadas.
* **Hallazgo**: El log muestra que **se eliminaron 2 filas** debido a fechas inválidas.

### Outliers y Limpieza de Montos (`amount`)
La columna `amount` (en moneda local) estaba en formato de texto, con símbolos (`$`, `€`) y comas decimales.
* **Proceso**: Se aplicó una limpieza (reemplazando `[$,€]` por `''` y `,` por `.`) y se convirtió a numérico.
* **Hallazgo**: El log indica que **0 filas** fueron eliminadas por montos inválidos, lo que significa que la limpieza fue 100% exitosa.
* **Análisis de Outliers (Moneda Local)**: El análisis descriptivo de la columna `amount` (antes de la conversión a USD) muestra:
    ```
    [EDA Txns] Análisis de Outliers ('amount'): 
    ...
    50%       278.480000
    99%      1999.000000
    ...
    [EDA Txns] Conteo de outliers (amount > 4999.0): 2
    ```
    Esto indica que el 99% de las transacciones son menores a $1999, pero existen al menos 2 transacciones extremas superiores a $4999.

**Resumen de Calidad:**
* Filas Iniciales en `transactions.csv`: 22,341
* Menos Duplicados: -10
* Menos Nulos Críticos: -110
* Menos Fechas Inválidas: -2
* **Total de Filas Limpias (Staging): 22,219**

---

## 2. Monedas

### Normalización a USD (Fase 1)
La normalización de moneda es un paso crítico realizado en `EDA.py`.
* **Proceso**: Se cargó `exchange_rates.csv`, se unió con las transacciones y se creó la columna `amount_usd = amount * rate_to_usd`.
* **Hallazgo (Distribución USD)**: El log `initial_cleaning_and_eda` proporciona las estadísticas descriptivas para la nueva columna `amount_usd` sobre las 22,219 filas limpias:
    ```
    [EDA Txns] Distribución de 'amount_usd': 
    count    22219.000000
    mean       141.149176
    std        282.839841
    min          1.160000
    25%         28.900000
    50%         63.800000
    75%        148.440000
    max       4999.000000
    ```
    Notablemente, la media ($141.15) es más del doble de la mediana ($63.80), confirmando una distribución con una fuerte cola derecha (skewed right).

### Visualización de Distribución (Fase 2)
Para entender esta distribución, `EDA_FINAL.py` genera dos histogramas:
1.  **Histograma (Escala Logarítmica)**: Muestra la distribución completa, confirmando la "larga cola" (long tail) de transacciones de alto valor.
2.  **Histograma (Quantile 95%)**: Muestra la "cabeza" de la distribución, es decir, la gran mayoría de las transacciones.

![Distribución Logarítmica](dist_amount_usd_log.png)
![Distribución Q95](dist_amount_usd_q95.png)

---

## 3. Clientes

### Cohortes por `signup_ts` (Fase 1)
El script `EDA.py` realiza un análisis de cohortes preliminar para entender la adquisición de clientes.
* **Proceso**: Se agruparon los clientes de `customers.csv` por `signup_month` (Mes de registro).
* **Hallazgo**: El log `initial_cleaning_and_eda` muestra los siguientes conteos de nuevos clientes por mes:
    ```
    [EDA Cust] Análisis de Cohortes (nuevos clientes/mes): 
    signup_month
    2025-05    285
    2025-06    275
    2025-07    287
    2025-08    300
    2025-09    282
    2025-10    316
    2025-11    285
    2025-12    284
    2026-01    304
    2026-02    292
    2026-03    290
    Name: count, dtype: int64
    ```

### Marketing Source por Región (Fase 2)
El script `EDA_FINAL.py` genera una visualización clave que cruza las ventas (USD) por región y fuente de marketing.
* **Hallazgo Clave**: El log `final_analysis_and_reporting` confirma que **NORTHAMERICA es el mercado más grande**:
    ```
    [Insight Cliente] La región 'NORTHAMERICA' es el mercado más grande.
    ```
* **Tabla de Ventas (USD) por Región y Fuente**:
    ```
    [final_analysis_and_reporting.log]
    marketing_source     direct       email     organic     partner        paid
    region                                                                    
    APAC                 0.00000  106883.3308   47481.5432  105307.7476  103138.8312
    EMEA                 0.00000  122420.2548   57493.1848  126384.8140  118544.7556
    LATAM           303253.94552  124891.1396   56667.1420  117398.9048  122069.9480
    NORTHAMERICA    612089.43160  135111.4320   64548.7484  132808.9048  132049.4936
    ```
    Este desglose revela que el dominio de NORTHAMERICA se debe casi en su totalidad a la fuente "direct", que está ausente en APAC y EMEA.
* **Visualización**: El gráfico de barras apiladas (corregido para mostrar "Millones USD") ilustra este hallazgo visualmente.

![Marketing por Región](client_marketing_region.png)

---

## 4. Productos

### Ticket Medio y Dispersión por Categoría (Fase 2)
* **Hallazgo Clave**: El log `final_analysis_and_reporting` identifica la categoría más importante por ingresos:
    ```
    [Insight Producto] La categoría 'computers' es la que más ingresos genera.
    ```
* **Tabla de Agregación por Categoría**:
    ```
    [final_analysis_and_reporting.log]
                          mean           sum   count
    category                                          
    computers       626.654359  1311584.0720    2093
    cellphones      408.887854   776478.0436    1899
    video games      41.385551   165956.0604    4009
    accessories      33.829377   154881.0924    4579
    software         51.571408   152135.6536    2950
    tablets         228.618055   147453.9496     645
    tv & audio      167.925070   126618.3304     754
    smart home       27.420609   110196.4020    4019
    ```
    `computers` no solo tiene el mayor volumen de ventas (`sum`), sino también el `mean` (ticket medio) más alto con $626.65.
* **Visualización**: El boxplot con escala logarítmica confirma que `computers` y `cellphones` tienen la mayor dispersión y los tickets medios más altos.

![Boxplot Categorías (Log Scale)](product_boxplot_category.png)

### Combinaciones Anómalas `country × category` (Fase 2)
Para identificar patrones de compra, se generó un heatmap (Top 20 países vs. Categoría).
* **Hallazgos**:
    * **Concentraciones Fuertes**: El heatmap muestra puntos calientes (amarillos) claros, destacando la alta demanda de `computers` y `cellphones` en `USA`.
    * **Oportunidades (Zonas Oscuras)**: Muestra una actividad notablemente baja para categorías como `tablets` y `tv & audio` en la mayoría de los países, lo que podría indicar una oportunidad de mercado o de cross-selling.

![Heatmap País x Categoría](product_country_category_heatmap.png)

---

## 5. Análisis Temporal

### Picos y Caídas por Día (Fase 1 y 2)
* **Proceso**: `EDA.py` analizó las ventas diarias agregadas, `EDA_FINAL.py` las graficó.
* **Hallazgo Clave (Anomalía)**: Se detectó un pico de ventas masivo.
    * El log `initial_cleaning_and_eda` muestra un `max` de **$365,042.75** en ventas diarias, comparado con una media de **$28,386.82**.
    * El log `final_analysis_and_reporting` identifica la fecha exacta:
        ```
        [Insight Temporal] El día pico de ventas fue 2025-08-01.
        ```
* **Visualización**: El gráfico de líneas temporales muestra claramente esta anomalía extrema el 1 de agosto de 2025, que distorsiona el resto de la gráfica y debe ser investigada (¿un evento de ventas, un error de datos, una carga masiva?).

![Ventas Diarias](temporal_daily_sales.png)

### Patrones por Día de la Semana (Fase 2)
* **Proceso**: `EDA_FINAL.py` agregó las ventas por día de la semana (`weekday`).
* **Hallazgo Clave**: El log `final_analysis_and_reporting.` identifica el día más fuerte:
    ```
    [Insight Temporal] El día de la semana con más ventas es Friday.
    ```
* **Tabla de Ventas por Día de la Semana**:
    ```
    [final_analysis_and_reporting.log]
    weekday
    Monday       397850.5560
    Tuesday      388755.9036
    Wednesday    402157.3496
    Thursday     414436.5612
    Friday       488344.0208
    Saturday     445037.1328
    Sunday       408722.0800
    ```
    El viernes es el líder claro, con un fin de semana (sábado) también fuerte. El martes parece ser el día más lento.
* **Visualización**: El gráfico de barras confirma este patrón semanal.

![Ventas Semanales](temporal_weekly_sales.png)