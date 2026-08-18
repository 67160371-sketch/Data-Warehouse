"""
Omnichannel Retail ETL Pipeline
================================
Incremental & Idempotent ETL: Extract -> Transform -> Validate -> Load
Source : Python_Data_Pipeline_Lab_Dataset.xlsx (customers, products, orders_batch_1..3)
Target : SQLite Star Schema (dim_customer, dim_product, dim_date, fact_sales)

Run:
    python pipeline.py
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("retail_pipeline")


# ==========================================================================
# Task 1 - PipelineConfig
# ==========================================================================
@dataclass
class PipelineConfig:
    """Central configuration for a pipeline run."""

    input_path: Path
    output_db: Path
    batches: list[int]
    error_mode: str = "quarantine"  # "quarantine" (default) or "fail_fast"
    quarantine_csv: Path = Path("quarantine.csv")
    run_log_csv: Path = Path("pipeline_run_log.csv")

    def __post_init__(self) -> None:
        self.input_path = Path(self.input_path)
        self.output_db = Path(self.output_db)
        self.quarantine_csv = Path(self.quarantine_csv)
        self.run_log_csv = Path(self.run_log_csv)
        if self.error_mode not in ("quarantine", "fail_fast"):
            raise ValueError("error_mode must be 'quarantine' or 'fail_fast'")
        if not self.input_path.exists():
            raise FileNotFoundError(f"input_path not found: {self.input_path}")


# ==========================================================================
# Normalization lookup tables (Task 2)
# ==========================================================================
PAYMENT_METHOD_MAP = {
    "cash": "Cash",
    "credit card": "Credit Card",
    "promptpay": "PromptPay",
    "bank transfer": "Bank Transfer",
}

SALES_CHANNEL_MAP = {
    "store": "Store",
    "online": "Online",
    "marketplace": "Marketplace",
    "e-commerce": "Online",  # explicit rule from data dictionary
}

REQUIRED_ORDER_COLUMNS = [
    "order_id", "order_datetime", "customer_id", "product_id", "quantity",
    "unit_price", "discount_pct", "payment_method", "sales_channel",
    "updated_at", "source_batch",
]


# ==========================================================================
# Task 1 - Extract
# ==========================================================================
def extract_sheet(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    """Read a single worksheet with logging + error handling. Never mutates source."""
    started = time.time()
    try:
        df = xl.parse(sheet_name)
        elapsed = time.time() - started
        logger.info(
            "EXTRACT ok    sheet=%-16s rows=%-5d elapsed=%.3fs", sheet_name, len(df), elapsed
        )
        return df
    except Exception as exc:  # noqa: BLE001
        elapsed = time.time() - started
        logger.error(
            "EXTRACT FAIL  sheet=%-16s elapsed=%.3fs error=%s", sheet_name, elapsed, exc
        )
        raise


def extract_dimensions(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract the customers and products dimension sources."""
    xl = pd.ExcelFile(config.input_path)
    customers = extract_sheet(xl, "customers")
    products = extract_sheet(xl, "products")
    return customers, products


def extract_batch(config: PipelineConfig, batch_no: int) -> pd.DataFrame:
    """Extract a single orders_batch_N sheet."""
    xl = pd.ExcelFile(config.input_path)
    sheet = f"orders_batch_{batch_no}"
    return extract_sheet(xl, sheet)


# ==========================================================================
# Task 2 - Transform + Data Quality
# ==========================================================================
def _clean_unit_price(series: pd.Series) -> pd.Series:
    """Strip 'THB' prefixes / whitespace and coerce to numeric."""
    cleaned = (
        series.astype(str)
        .str.replace("THB", "", regex=False)
        .str.strip()
        .replace({"nan": None, "None": None})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _clean_quantity(series: pd.Series) -> pd.Series:
    """Coerce quantity to numeric; non-numeric text (e.g. 'three') becomes NaN."""
    return pd.to_numeric(series, errors="coerce")


def transform_orders(raw: pd.DataFrame, batch_no: int) -> pd.DataFrame:
    """Type-safe conversions + label normalization. Does not validate business rules yet."""
    df = raw.copy()

    df["order_datetime_parsed"] = pd.to_datetime(df["order_datetime"], errors="coerce")
    df["updated_at_parsed"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["quantity_clean"] = _clean_quantity(df["quantity"])
    df["unit_price_clean"] = _clean_unit_price(df["unit_price"])
    df["discount_pct_clean"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    df["payment_method_clean"] = (
        df["payment_method"].astype(str).str.strip().str.lower().map(PAYMENT_METHOD_MAP)
    )
    df["sales_channel_clean"] = (
        df["sales_channel"].astype(str).str.strip().str.lower().map(SALES_CHANNEL_MAP)
    )

    df["customer_id"] = df["customer_id"].astype("string").str.strip()
    df["product_id"] = df["product_id"].astype("string").str.strip()
    df["source_batch"] = batch_no
    return df


def deduplicate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Keep only the record with the latest updated_at per order_id (within this batch).
    Older duplicate copies are returned separately as 'superseded' rows so they can be
    quarantined with an explicit reason_code (kept inside the read/valid/rejected formula).
    """
    df = df.sort_values("updated_at_parsed", ascending=False, na_position="last")
    is_dup = df.duplicated(subset=["order_id"], keep="first")
    kept = df[~is_dup].copy()
    superseded = df[is_dup].copy()
    if len(superseded):
        logger.info("DEDUP         superseded_duplicates=%d", len(superseded))
    return kept, superseded


def validate_orders(
    df: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply business rules row by row (vectorized). Returns (clean_df, quarantine_df).
    Every rejected row carries one or more reason_code values (semicolon separated).
    """
    valid_customer_ids = set(customers["customer_id"])
    active_product_ids = set(products.loc[products["active_flag"] == "Y", "product_id"])
    all_product_ids = set(products["product_id"])

    reasons = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)

    def flag(mask: pd.Series, code: str) -> None:
        for idx in df.index[mask]:
            reasons.loc[idx].append(code)

    flag(df["customer_id"].isna() | (df["customer_id"] == ""), "missing_customer_id")
    flag(
        df["customer_id"].notna() & (df["customer_id"] != "") & ~df["customer_id"].isin(valid_customer_ids),
        "customer_not_found",
    )
    flag(df["product_id"].isna() | (df["product_id"] == ""), "missing_product_id")
    flag(
        df["product_id"].notna() & (df["product_id"] != "") & ~df["product_id"].isin(all_product_ids),
        "product_not_found",
    )
    flag(df["product_id"].isin(all_product_ids - active_product_ids), "inactive_product")

    flag(df["quantity_clean"].isna(), "invalid_quantity")
    flag(df["quantity_clean"].notna() & ~df["quantity_clean"].between(1, 20), "invalid_quantity")

    flag(df["unit_price_clean"].isna(), "invalid_unit_price")
    flag(df["unit_price_clean"].notna() & (df["unit_price_clean"] <= 0), "invalid_unit_price")

    flag(df["discount_pct_clean"].isna(), "invalid_discount_pct")
    flag(
        df["discount_pct_clean"].notna() & ~df["discount_pct_clean"].between(0, 100),
        "invalid_discount_pct",
    )

    flag(df["order_datetime_parsed"].isna(), "invalid_order_datetime")
    flag(df["updated_at_parsed"].isna(), "invalid_updated_at")
    flag(
        df["payment_method_clean"].isna() | (df["payment_method_clean"] == ""),
        "invalid_payment_method",
    )
    flag(
        df["sales_channel_clean"].isna() | (df["sales_channel_clean"] == ""),
        "invalid_sales_channel",
    )
    flag(df["order_id"].isna() | (df["order_id"].astype(str).str.strip() == ""), "missing_order_id")

    df = df.copy()
    df["reason_code"] = reasons.apply(lambda r: ";".join(sorted(set(r))) if r else "")

    is_bad = df["reason_code"] != ""
    quarantine_df = df[is_bad].copy()
    clean_df = df[~is_bad].copy()

    if len(clean_df):
        clean_df["gross_amount"] = (clean_df["quantity_clean"] * clean_df["unit_price_clean"]).round(2)
        clean_df["net_amount"] = (
            clean_df["gross_amount"] * (1 - clean_df["discount_pct_clean"] / 100)
        ).round(2)

    return clean_df, quarantine_df


def build_quarantine_records(
    quarantine_df: pd.DataFrame, superseded_df: pd.DataFrame, batch_no: int
) -> pd.DataFrame:
    """Combine business-rule rejects and superseded duplicates into one quarantine table."""
    records = []
    now = datetime.now(timezone.utc).isoformat()

    for _, row in quarantine_df.iterrows():
        records.append(
            {
                "order_id": row.get("order_id"),
                "source_batch": batch_no,
                "reason_code": row["reason_code"],
                "raw_data": row[REQUIRED_ORDER_COLUMNS].to_json(force_ascii=False)
                if set(REQUIRED_ORDER_COLUMNS).issubset(row.index)
                else "{}",
                "quarantined_at": now,
            }
        )

    for _, row in superseded_df.iterrows():
        records.append(
            {
                "order_id": row.get("order_id"),
                "source_batch": batch_no,
                "reason_code": "superseded_duplicate",
                "raw_data": row[REQUIRED_ORDER_COLUMNS].to_json(force_ascii=False)
                if set(REQUIRED_ORDER_COLUMNS).issubset(row.index)
                else "{}",
                "quarantined_at": now,
            }
        )

    return pd.DataFrame(records)


# ==========================================================================
# Task 3 - Star Schema DDL
# ==========================================================================
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id  TEXT NOT NULL UNIQUE,
        customer_name TEXT,
        province     TEXT,
        segment      TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key  INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id   TEXT NOT NULL UNIQUE,
        product_name TEXT,
        category     TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key   INTEGER PRIMARY KEY,
        full_date  TEXT NOT NULL UNIQUE,
        day        INTEGER,
        month      INTEGER,
        quarter    INTEGER,
        year       INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_sales (
        order_id       TEXT PRIMARY KEY,
        date_key       INTEGER NOT NULL REFERENCES dim_date(date_key),
        customer_key   INTEGER NOT NULL REFERENCES dim_customer(customer_key),
        product_key    INTEGER NOT NULL REFERENCES dim_product(product_key),
        quantity       INTEGER NOT NULL CHECK (quantity > 0),
        unit_price     REAL NOT NULL CHECK (unit_price > 0),
        discount_pct   REAL NOT NULL CHECK (discount_pct BETWEEN 0 AND 100),
        gross_amount   REAL NOT NULL CHECK (gross_amount >= 0),
        net_amount     REAL NOT NULL CHECK (net_amount >= 0),
        payment_method TEXT,
        sales_channel  TEXT,
        source_batch   INTEGER,
        updated_at     TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS quarantine (
        quarantine_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id       TEXT,
        source_batch   INTEGER,
        reason_code    TEXT NOT NULL,
        raw_data       TEXT,
        quarantined_at TEXT,
        UNIQUE(order_id, source_batch, reason_code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        batch            INTEGER,
        started_at       TEXT,
        ended_at         TEXT,
        rows_read        INTEGER,
        rows_valid       INTEGER,
        rows_rejected    INTEGER,
        rows_duplicated  INTEGER,
        rows_loaded      INTEGER,
        status           TEXT
    );
    """,
]


def init_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)
    conn.commit()


# ==========================================================================
# Task 3 - Load
# ==========================================================================
def load_dim_customer(conn: sqlite3.Connection, customers: pd.DataFrame) -> None:
    rows = list(
        customers[["customer_id", "customer_name", "province", "segment"]].itertuples(
            index=False, name=None
        )
    )
    conn.executemany(
        "INSERT OR IGNORE INTO dim_customer (customer_id, customer_name, province, segment) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def load_dim_product(conn: sqlite3.Connection, products: pd.DataFrame) -> None:
    rows = list(
        products[["product_id", "product_name", "category"]].itertuples(index=False, name=None)
    )
    conn.executemany(
        "INSERT OR IGNORE INTO dim_product (product_id, product_name, category) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()


def load_dim_date(conn: sqlite3.Connection, clean_df: pd.DataFrame) -> None:
    if clean_df.empty:
        return
    dates = clean_df["order_datetime_parsed"].dt.normalize().dropna().unique()
    rows = []
    for d in dates:
        ts = pd.Timestamp(d)
        date_key = int(ts.strftime("%Y%m%d"))
        rows.append(
            (
                date_key,
                ts.strftime("%Y-%m-%d"),
                ts.day,
                ts.month,
                (ts.month - 1) // 3 + 1,
                ts.year,
            )
        )
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def load_fact_sales(conn: sqlite3.Connection, clean_df: pd.DataFrame, error_mode: str) -> tuple[int, pd.DataFrame]:
    """
    Idempotent upsert keyed by order_id:
      - new order_id           -> INSERT, counts as loaded
      - existing, newer update -> UPDATE, counts as loaded
      - existing, same/older   -> SKIP, does not grow fact table (idempotency)
    Any unexpected per-row DB failure is quarantined instead of aborting the whole batch,
    so already-loaded rows are never rolled back (Task 5 requirement).
    """
    if clean_df.empty:
        return 0, pd.DataFrame(columns=["order_id", "reason_code"])

    cur = conn.cursor()
    cust_map = dict(cur.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
    prod_map = dict(cur.execute("SELECT product_id, product_key FROM dim_product").fetchall())
    existing_updated = dict(cur.execute("SELECT order_id, updated_at FROM fact_sales").fetchall())

    loaded = 0
    load_failures = []

    for _, row in clean_df.iterrows():
        order_id = row["order_id"]
        new_updated_at = row["updated_at_parsed"].isoformat()
        date_key = int(row["order_datetime_parsed"].strftime("%Y%m%d"))
        customer_key = cust_map.get(row["customer_id"])
        product_key = prod_map.get(row["product_id"])

        if customer_key is None or product_key is None:
            load_failures.append((order_id, "dimension_key_lookup_failed"))
            continue

        prior = existing_updated.get(order_id)
        if prior is not None and prior >= new_updated_at:
            continue  # unchanged -> idempotent skip

        try:
            conn.execute(
                """
                INSERT INTO fact_sales (
                    order_id, date_key, customer_key, product_key, quantity, unit_price,
                    discount_pct, gross_amount, net_amount, payment_method, sales_channel,
                    source_batch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    date_key=excluded.date_key,
                    customer_key=excluded.customer_key,
                    product_key=excluded.product_key,
                    quantity=excluded.quantity,
                    unit_price=excluded.unit_price,
                    discount_pct=excluded.discount_pct,
                    gross_amount=excluded.gross_amount,
                    net_amount=excluded.net_amount,
                    payment_method=excluded.payment_method,
                    sales_channel=excluded.sales_channel,
                    source_batch=excluded.source_batch,
                    updated_at=excluded.updated_at
                """,
                (
                    order_id,
                    date_key,
                    int(customer_key),
                    int(product_key),
                    int(row["quantity_clean"]),
                    float(row["unit_price_clean"]),
                    float(row["discount_pct_clean"]),
                    float(row["gross_amount"]),
                    float(row["net_amount"]),
                    row["payment_method_clean"],
                    row["sales_channel_clean"],
                    int(row["source_batch"]),
                    new_updated_at,
                ),
            )
            loaded += 1
        except sqlite3.Error as exc:
            conn.rollback()
            logger.error("LOAD FAIL     order_id=%s error=%s", order_id, exc)
            load_failures.append((order_id, f"db_error:{exc}"))
            if error_mode == "fail_fast":
                raise

    conn.commit()
    failures_df = pd.DataFrame(load_failures, columns=["order_id", "reason_code"])
    return loaded, failures_df


def load_quarantine(conn: sqlite3.Connection, quarantine_records: pd.DataFrame) -> None:
    if quarantine_records.empty:
        return
    rows = list(
        quarantine_records[["order_id", "source_batch", "reason_code", "raw_data", "quarantined_at"]]
        .itertuples(index=False, name=None)
    )
    conn.executemany(
        "INSERT OR IGNORE INTO quarantine (order_id, source_batch, reason_code, raw_data, quarantined_at) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def log_run(conn: sqlite3.Connection, entry: dict) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_run_log
            (batch, started_at, ended_at, rows_read, rows_valid, rows_rejected,
             rows_duplicated, rows_loaded, status)
        VALUES (:batch, :started_at, :ended_at, :rows_read, :rows_valid, :rows_rejected,
                :rows_duplicated, :rows_loaded, :status)
        """,
        entry,
    )
    conn.commit()


# ==========================================================================
# Task 5 - Orchestration
# ==========================================================================
def process_batch(conn: sqlite3.Connection, config: PipelineConfig, batch_no: int) -> dict:
    """Run extract -> transform -> validate -> load for a single batch. Never raises
    (unless error_mode='fail_fast'); failures are captured in the run log with status='failed'."""
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("BATCH START   batch=%d", batch_no)

    try:
        raw = extract_batch(config, batch_no)
        rows_read = len(raw)

        transformed = transform_orders(raw, batch_no)
        kept, superseded = deduplicate_orders(transformed)
        rows_duplicated = len(superseded)

        customers, products = extract_dimensions(config)
        clean_df, rejected_df = validate_orders(kept, customers, products)

        quarantine_records = build_quarantine_records(rejected_df, superseded, batch_no)
        load_quarantine(conn, quarantine_records)

        load_dim_date(conn, clean_df)
        loaded, load_failures = load_fact_sales(conn, clean_df, config.error_mode)
        if len(load_failures):
            extra_q = load_failures.copy()
            extra_q["source_batch"] = batch_no
            extra_q["raw_data"] = "{}"
            extra_q["quarantined_at"] = datetime.now(timezone.utc).isoformat()
            load_quarantine(conn, extra_q[["order_id", "source_batch", "reason_code", "raw_data", "quarantined_at"]])

        rows_valid = len(clean_df)
        rows_rejected = len(rejected_df) + rows_duplicated + len(load_failures)
        status = "success"

    except Exception as exc:  # noqa: BLE001
        logger.error("BATCH FAILED  batch=%d error=%s", batch_no, exc)
        ended_at = datetime.now(timezone.utc).isoformat()
        entry = {
            "batch": batch_no, "started_at": started_at, "ended_at": ended_at,
            "rows_read": 0, "rows_valid": 0, "rows_rejected": 0,
            "rows_duplicated": 0, "rows_loaded": 0, "status": f"failed:{exc}",
        }
        log_run(conn, entry)
        if config.error_mode == "fail_fast":
            raise
        return entry

    ended_at = datetime.now(timezone.utc).isoformat()
    entry = {
        "batch": batch_no, "started_at": started_at, "ended_at": ended_at,
        "rows_read": rows_read, "rows_valid": rows_valid, "rows_rejected": rows_rejected,
        "rows_duplicated": rows_duplicated, "rows_loaded": loaded, "status": status,
    }
    log_run(conn, entry)
    logger.info(
        "BATCH DONE    batch=%d read=%d valid=%d rejected=%d duplicated=%d loaded=%d status=%s",
        batch_no, rows_read, rows_valid, rows_rejected, rows_duplicated, loaded, status,
    )
    return entry


def run_pipeline(config: PipelineConfig) -> list[dict]:
    """Orchestrator: extract -> transform -> validate -> load for every batch in config.batches."""
    conn = sqlite3.connect(config.output_db)
    init_schema(conn)

    customers, products = extract_dimensions(config)
    load_dim_customer(conn, customers)
    load_dim_product(conn, products)

    results = []
    for batch_no in config.batches:
        # dim_date needs a peek at this batch's valid rows too; process_batch loads fact rows
        # and we top up dim_date right before that inside process_batch via clean_df.
        entry = process_batch(conn, config, batch_no)
        results.append(entry)

    conn.close()
    return results


# ==========================================================================
# KPI summary
# ==========================================================================
def print_kpi_summary(db_path: Path) -> dict:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    total_read = cur.execute("SELECT COALESCE(SUM(rows_read),0) FROM pipeline_run_log").fetchone()[0]
    total_valid = cur.execute("SELECT COALESCE(SUM(rows_valid),0) FROM pipeline_run_log").fetchone()[0]
    total_rejected = cur.execute("SELECT COALESCE(SUM(rows_rejected),0) FROM pipeline_run_log").fetchone()[0]
    total_duplicated = cur.execute("SELECT COALESCE(SUM(rows_duplicated),0) FROM pipeline_run_log").fetchone()[0]
    total_loaded = cur.execute("SELECT COALESCE(SUM(rows_loaded),0) FROM pipeline_run_log").fetchone()[0]
    fact_rows = cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    net_revenue = cur.execute("SELECT COALESCE(SUM(net_amount),0) FROM fact_sales").fetchone()[0]
    conn.close()

    kpi = {
        "rows_read_total": total_read,
        "rows_valid_total": total_valid,
        "rows_rejected_total": total_rejected,
        "rows_duplicated_total": total_duplicated,
        "rows_loaded_total": total_loaded,
        "fact_sales_row_count": fact_rows,
        "net_revenue_total": round(net_revenue, 2),
    }
    logger.info("KPI SUMMARY   %s", json.dumps(kpi, ensure_ascii=False))
    return kpi


# ==========================================================================
# Export helpers (deliverables)
# ==========================================================================
def export_csv_tables(db_path: Path, config: PipelineConfig) -> None:
    conn = sqlite3.connect(db_path)
    quarantine_df = pd.read_sql("SELECT * FROM quarantine ORDER BY quarantine_id", conn)
    run_log_df = pd.read_sql("SELECT * FROM pipeline_run_log ORDER BY run_id", conn)
    conn.close()
    quarantine_df.to_csv(config.quarantine_csv, index=False, encoding="utf-8-sig")
    run_log_df.to_csv(config.run_log_csv, index=False, encoding="utf-8-sig")
    logger.info("EXPORT        %s (%d rows)", config.quarantine_csv, len(quarantine_df))
    logger.info("EXPORT        %s (%d rows)", config.run_log_csv, len(run_log_df))


# ==========================================================================
# Demo entry point: batch_1 -> batch_1 (rerun) -> batch_2 -> batch_3
# ==========================================================================
def main() -> None:
    db_path = Path("retail_dw.db")
    if db_path.exists():
        db_path.unlink()  # fresh database for a clean demo run

    base_config = dict(
        input_path="source_dataset.xlsx",
        output_db=db_path,
        error_mode="quarantine",
        quarantine_csv="quarantine.csv",
        run_log_csv="pipeline_run_log.csv",
    )

    logger.info("=" * 70)
    logger.info("RUN 1: batch_1 (first load)")
    run_pipeline(PipelineConfig(batches=[1], **base_config))

    logger.info("=" * 70)
    logger.info("RUN 2: batch_1 (rerun - must be idempotent, fact rows must NOT grow)")
    run_pipeline(PipelineConfig(batches=[1], **base_config))

    logger.info("=" * 70)
    logger.info("RUN 3: batch_2 (incremental load)")
    run_pipeline(PipelineConfig(batches=[2], **base_config))

    logger.info("=" * 70)
    logger.info("RUN 4: batch_3 (incremental load)")
    run_pipeline(PipelineConfig(batches=[3], **base_config))

    logger.info("=" * 70)
    kpi = print_kpi_summary(db_path)

    export_csv_tables(db_path, PipelineConfig(batches=[], **base_config))

    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE. KPI = %s", kpi)


if __name__ == "__main__":
    main()
