import pandas as pd
from pathlib import Path

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

def read_all():
    tx  = pd.read_csv(RAW / "transactions.csv", dtype=str)
    cu  = pd.read_csv(RAW / "customers.csv", dtype=str)
    pr  = pd.read_csv(RAW / "products.csv", dtype=str)
    fx  = pd.read_csv(RAW / "exchange_rates.csv", dtype=str)
    cr  = pd.read_csv(RAW / "country_region.csv", dtype=str)
    return tx, cu, pr, fx, cr
