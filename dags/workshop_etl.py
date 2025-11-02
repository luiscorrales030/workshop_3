from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.append("/opt/airflow/etl")
from extract import read_all
from transform import transform
from load import save_csv, save_sqlite

default_args = {
    "owner": "workshop",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="workshop_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["workshop","etl"],
) as dag:

    def _extract(**context):
        tx, cu, pr, fx, cr = read_all()
        context["ti"].xcom_push(key="tx", value=tx.to_json(orient="records"))
        context["ti"].xcom_push(key="cu", value=cu.to_json(orient="records"))
        context["ti"].xcom_push(key="pr", value=pr.to_json(orient="records"))
        context["ti"].xcom_push(key="fx", value=fx.to_json(orient="records"))
        context["ti"].xcom_push(key="cr", value=cr.to_json(orient="records"))

    def _transform(**context):
        import pandas as pd
        ti = context["ti"]
        tx = pd.read_json(ti.xcom_pull(key="tx"), orient="records")
        cu = pd.read_json(ti.xcom_pull(key="cu"), orient="records")
        pr = pd.read_json(ti.xcom_pull(key="pr"), orient="records")
        fx = pd.read_json(ti.xcom_pull(key="fx"), orient="records")
        cr = pd.read_json(ti.xcom_pull(key="cr"), orient="records")
        fact, daily = transform(tx, cu, pr, fx, cr)
        ti.xcom_push(key="fact", value=fact.to_json(orient="records", date_format="iso"))
        ti.xcom_push(key="daily", value=daily.to_json(orient="records", date_format="iso"))

    def _load(**context):
        import pandas as pd
        ti = context["ti"]
        fact = pd.read_json(ti.xcom_pull(key="fact"), orient="records")
        daily = pd.read_json(ti.xcom_pull(key="daily"), orient="records")
        save_csv(fact, daily)
        save_sqlite(fact, daily)

    t1 = PythonOperator(task_id="extract", python_callable=_extract)
    t2 = PythonOperator(task_id="transform", python_callable=_transform)
    t3 = PythonOperator(task_id="load", python_callable=_load)

    t1 >> t2 >> t3
