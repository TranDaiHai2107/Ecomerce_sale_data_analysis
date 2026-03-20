"""
ETL Pipeline for Office Furniture E-commerce Data
==================================================
Extract raw CSVs → Transform (clean, parse, merge) → Load to processed/
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


# ── EXTRACT ──────────────────────────────────────────────────────────────────

def extract() -> dict[str, pd.DataFrame]:
    """Load all raw CSV files into DataFrames."""
    logger.info("Extracting raw data...")
    tables = {}
    for name in ["orders", "customer", "product", "payment", "shipping"]:
        path = os.path.join(RAW_DIR, f"{name}.csv")
        tables[name] = pd.read_csv(path)
        logger.info(f"  {name}: {tables[name].shape[0]:,} rows × {tables[name].shape[1]} cols")
    return tables


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def _clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the orders table:
    1. Parse `'quantity':'cost'` → separate quantity & cost columns
    2. Standardise order_status (Completed / Cancelled)
    3. Convert order_date to datetime
    4. Rename columns to snake_case
    """
    df = df.copy()

    # ── Column rename ────────────────────────────────────────────────────
    df.columns = [c.strip() for c in df.columns]
    rename_map = {
        "order id": "order_id",
        "customer id": "customer_id",
        "product id": "product_id",
        "order status": "order_status",
        "payment id": "payment_id",
        "order date": "order_date",
        "shipping id": "shipping_id",
        "Is old customer": "is_old_customer",
        "'quantity':'cost'": "quantity_cost_raw",
    }
    df.rename(columns=rename_map, inplace=True)

    # ── Parse quantity & cost from "'7': '126.82'" format ────────────────
    def _parse_qty_cost(val):
        try:
            val = str(val).replace("'", "").replace('"', "")
            parts = val.split(":")
            qty = int(parts[0].strip())
            cost = float(parts[1].strip())
            return pd.Series({"quantity": qty, "cost": cost})
        except Exception:
            return pd.Series({"quantity": np.nan, "cost": np.nan})

    parsed = df["quantity_cost_raw"].apply(_parse_qty_cost)
    df["quantity"] = parsed["quantity"].astype("Int64")
    df["cost"] = parsed["cost"].astype(float)
    df.drop(columns=["quantity_cost_raw"], inplace=True)

    # ── Standardise order_status ─────────────────────────────────────────
    df["order_status"] = df["order_status"].str.strip().str.capitalize()

    # ── Convert order_date ───────────────────────────────────────────────
    df["order_date"] = pd.to_datetime(df["order_date"], format="mixed", dayfirst=False)

    # ── is_old_customer → boolean ────────────────────────────────────────
    df["is_old_customer"] = df["is_old_customer"].str.strip().str.lower().map(
        {"yes": True, "no": False}
    )

    # ── feedback as int ──────────────────────────────────────────────────
    df["feedback"] = pd.to_numeric(df["feedback"], errors="coerce").astype("Int64")

    logger.info(f"  orders cleaned: {df.shape[0]:,} rows")
    return df


def _clean_customer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={"customer id": "customer_id"}, inplace=True)
    df["gender"] = df["gender"].str.strip().str.capitalize()
    df.drop_duplicates(subset=["customer_id"], inplace=True)
    logger.info(f"  customer cleaned: {df.shape[0]:,} rows")
    return df


def _clean_product(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={"product id": "product_id", "product type": "product_type", "unit price": "unit_price"}, inplace=True)
    df.drop_duplicates(subset=["product_id"], inplace=True)
    logger.info(f"  product cleaned: {df.shape[0]:,} rows")
    return df


def _clean_payment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={"payment_id": "payment_id", "payment method": "payment_method", "payment type": "payment_type"}, inplace=True)
    logger.info(f"  payment cleaned: {df.shape[0]:,} rows")
    return df


def _clean_shipping(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df.rename(columns={
        "shipping id": "shipping_id",
        "shipping type": "shipping_type",
        "shipping category": "shipping_category",
        "shipping cost level": "shipping_cost_level",
    }, inplace=True)
    logger.info(f"  shipping cleaned: {df.shape[0]:,} rows")
    return df


def transform(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Apply cleaning transformations to every table."""
    logger.info("Transforming data...")
    return {
        "orders": _clean_orders(tables["orders"]),
        "customer": _clean_customer(tables["customer"]),
        "product": _clean_product(tables["product"]),
        "payment": _clean_payment(tables["payment"]),
        "shipping": _clean_shipping(tables["shipping"]),
    }


def build_fact_table(cleaned: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all dimension tables into a single fact table for analysis.
    Star-schema: orders (fact) ← customer, product, payment, shipping (dims)
    """
    logger.info("Building merged fact table...")
    fact = (
        cleaned["orders"]
        .merge(cleaned["customer"], on="customer_id", how="left")
        .merge(cleaned["product"], on="product_id", how="left")
        .merge(cleaned["payment"], on="payment_id", how="left")
        .merge(cleaned["shipping"], on="shipping_id", how="left")
    )

    fact["revenue"] = fact["quantity"] * fact["unit_price"]

    logger.info(f"  fact table: {fact.shape[0]:,} rows × {fact.shape[1]} cols")
    return fact


# ── LOAD ─────────────────────────────────────────────────────────────────────

def load(cleaned: dict[str, pd.DataFrame], fact: pd.DataFrame) -> None:
    """Write cleaned tables and fact table to data/processed/."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    logger.info("Loading processed data...")

    for name, df in cleaned.items():
        out = os.path.join(PROCESSED_DIR, f"{name}_cleaned.csv")
        df.to_csv(out, index=False)
        logger.info(f"  → {out}")

    fact_path = os.path.join(PROCESSED_DIR, "fact_orders.csv")
    fact.to_csv(fact_path, index=False)
    logger.info(f"  → {fact_path}")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def run_pipeline():
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("ETL Pipeline started")
    logger.info("=" * 60)

    raw = extract()
    cleaned = transform(raw)
    fact = build_fact_table(cleaned)
    load(cleaned, fact)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Pipeline completed in {elapsed:.1f}s ✓")
    return cleaned, fact


if __name__ == "__main__":
    run_pipeline()
