import pandas as pd
import numpy as np

STATUS_MAP = {
    "paid":"paid","Paid":"paid","PAID":"paid",
    "cancelled":"cancelled",
    "refunded":"refunded","Refunded":"refunded"
}

def parse_ts(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, errors="coerce", utc=True, infer_datetime_format=True)
    bad = out.isna() & s.notna()
    if bad.any():
        alt = pd.to_datetime(s[bad], format="%Y/%m/%d %H:%M", errors="coerce", utc=True)
        out.loc[bad] = alt
    return out

def normalize_amount_col(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip()
    t = t.str.replace("$","", regex=False).str.replace("€","", regex=False)
    t = t.str.replace(",", ".", regex=False)
    t = t.replace({"": np.nan})
    return pd.to_numeric(t, errors="coerce")

def transform(tx, cu, pr, fx, cr):
    tx = tx.copy(); cu = cu.copy(); pr = pr.copy(); fx = fx.copy(); cr = cr.copy()

    tx["amount"] = normalize_amount_col(tx["amount"])
    tx["ts"] = parse_ts(tx["ts"])
    tx["status"] = tx["status"].map(STATUS_MAP)
    tx = tx.dropna(subset=["txn_id","customer_id","product_id","ts","status"])
    tx = (tx.sort_values("ts").drop_duplicates(subset=["txn_id"], keep="first"))

    cu["signup_ts"] = pd.to_datetime(cu["signup_ts"], errors="coerce", utc=True)
    df = tx.merge(cu, on="customer_id", how="left", validate="m:1")
    df = df.merge(pr, on="product_id", how="left", validate="m:1", suffixes=("","_prod"))
    df = df.merge(cr, on="country", how="left", validate="m:1")

    df["price_list"] = pd.to_numeric(df["price_list"], errors="coerce")

    fx["rate_to_usd"] = pd.to_numeric(fx["rate_to_usd"], errors="coerce")
    df = df.merge(fx, on="currency", how="left", validate="m:1")
    df["amount_usd"] = df["amount"] * df["rate_to_usd"]

    df["date"] = df["ts"].dt.date
    df["month"] = df["ts"].dt.to_period("M").astype(str)

    fact = df[["txn_id","customer_id","product_id","ts","date","month","country","region","category","status","currency","amount","amount_usd"]].copy()

    daily = (fact.query("status == 'paid'")
                  .groupby(["date","region","country","category"], as_index=False)
                  .agg(total_usd=("amount_usd","sum"), n=("txn_id","size")))

    return fact, daily
