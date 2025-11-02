# Workshop Final Final No da Mas — ETL Orquestado con Airflow (Kafka opcional)

**Objetivo**: Construir y ejecutar un pipeline **ETL** en **Airflow** usando el dataset proporcionado.  
> Kafka es **opcional** (no incluido en este starter); el foco aquí es **Airflow + ETL**.  
> **No** incluir pruebas unitarias en este workshop.

## 1) Entregables
1. `dags/workshop_etl.py` + código en `etl/`.
2. `docker-compose.yml` + `.env` (a partir de `.env.example`).
3. **EDA** con **hallazgos** (notebook o Markdown).

## 2) Dataset
- `transactions.csv` (~22350 filas, duplicados y formatos mixtos).
- `customers.csv` (~3500 filas, algunos nulos).
- `products.csv` (120 productos, 7 categorías).
- `exchange_rates.csv`, `country_region.csv`.

## 3) Instrucciones rápidas
```bash
# 1) Genera FERNET y colócalo en .env
python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY

# 2) Levanta servicios
docker compose up -d

# 3) UI Airflow: http://localhost:8080  (admin/admin)
#    Activa y ejecuta el DAG 'workshop_etl'
```

## 4) Puntos de EDA (resumido)
- Calidad: nulos, duplicados por `txn_id`, fechas inválidas, outliers.
- Monedas: normalización a USD, cambios de distribución, regiones líderes.
- Clientes: cohortes por `signup_ts`, marketing_source por región.
- Productos: ticket medio y dispersión por categoría; combinaciones anómalas `country × category`.
- Temporal: picos/caídas por día/semana.

## 5) Checklist de entrega (para el equipo)

**Antes de entregar, verifica:**
- [ ] `docker compose up -d` levanta sin errores (Airflow + Postgres).
- [ ] Accedes a Airflow en `http://localhost:8080` (usuario `admin` / `admin`).
- [ ] El DAG **`workshop_etl`** aparece activo y corre de inicio a fin.
- [ ] Se generan artefactos en `data/output/` (`fact_transactions.csv`, `agg_daily.csv`) **y/o** `warehouse.sqlite` con ambas tablas.
- [ ] El README del equipo explica **paso a paso** cómo reproducir (incluye `.env`/FERNET).
- [ ] El EDA incluye **hallazgos claros** con visualizaciones y responde a las preguntas propuestas.
- [ ] Logs del DAG con **conteos clave** (filas leídas/descartadas, agregaciones, tiempos).
- [ ] (Opcional) Extensión aplicada (ej.: reporte adicional o integración extra).

---

## 6) Rúbrica de evaluación (100 pts + bonus)

> El workshop es más sencillo que el proyecto final, pero mantiene los mismos componentes básicos. La nota total es sobre **100 puntos** con hasta **+5 pts** de bonus por extensiones.

### A. Reproducibilidad y entorno (20 pts)
- **Docker Compose/Arranque** (8): servicios levantan sin errores; `.env` correcto (FERNET válido); puertos documentados.
- **README claro** (8): pasos exactos, pre‑requisitos, cómo verificar salidas; troubleshooting mínimo.
- **Versionado** (4): versiones de imágenes/contenedores especificadas o justificadas.

### B. DAG de Airflow (30 pts)
- **Diseño de tareas** (10): separación `extract` → `transform` → `load`; dependencias correctas; nombres claros.
- **Configuración** (8): `start_date`, `schedule_interval`, `catchup`, `retries`/`retry_delay` coherentes con el caso.
- **Idempotencia** (6): re‑ejecuciones no duplican/corrompen outputs (evidencia en código o README).
- **Observabilidad** (6): logs con métricas (filtrados, nulos, deduplicados, filas escritas, tiempos).

### C. ETL (Transformaciones y Carga) (30 pts)
- **Limpieza y normalización** (10): parseo robusto de `ts` (formatos mixtos), normalización de `amount` (símbolos/comas), estandarización de `status`.
- **Integración y reglas** (10): joins con `customers`, `products`, `country_region`; conversión a USD; deduplicación por `txn_id` con criterio consistente.
- **Agregaciones y salidas** (6): métricas diarias por `region/país/categoría` (`n`, `total_usd`); escritura a CSV/SQLite.
- **Calidad del código** (4): estructura modular (`etl/`), funciones claras, manejo de errores razonable.

### D. EDA y hallazgos (20 pts)
- **Profundidad** (7): responde a calidad de datos, monedas, clientes, productos y temporalidad; identifica outliers/anomalías.
- **Visualizaciones y tablas** (5): adecuadas, legibles y relevantes para la narrativa.
- **Conclusiones** (8): insights accionables/hipótesis bien argumentadas.


### Bonus (hasta +5 pts)
- **Extensión opcional**: pequeño reporte adicional, parámetros en DAG, o una integración extra sensata (no se requiere Kafka para el workshop).

---

### Criterios de descuento (guía)
- No se puede ejecutar el entorno (‑15 a ‑30).
- DAG corre con errores o sin outputs claros (‑10 a ‑25).
- EDA superficial o sin conclusiones (‑5 a ‑15).
- README insuficiente para reproducir (‑5 a ‑15).

---

