import pandas as pd
import sqlite3
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "output"
OUT.mkdir(parents=True, exist_ok=True)

def save_csv(fact: pd.DataFrame, daily: pd.DataFrame):
    fact.to_csv(OUT/"fact_transactions.csv", index=False)
    daily.to_csv(OUT/"agg_daily.csv", index=False)

def save_sqlite(fact: pd.DataFrame, daily: pd.DataFrame, db_path: str=None):
    if db_path is None:
        db_path = str(OUT/"warehouse.sqlite")
    con = sqlite3.connect(db_path)
    fact.to_sql("fact_transactions", con, if_exists="replace", index=False)
    daily.to_sql("agg_daily", con, if_exists="replace", index=False)
    con.close()
