"""
Pipeline Executor - Loads synthetic data into BigQuery and runs RAW → CDL transforms.

Steps:
  1. Create BigQuery datasets (Src_NovaStar, CDL_NovaStar)
  2. Create RAW tables via DDL
  3. Load JSONL data into RAW tables
  4. Create CDL tables via DDL
  5. Run RAW → CDL transformation SQL

Usage:
    python -m synthetic_data.executor --project m-mapping-gen-2026

    # Run individual steps:
    python -m synthetic_data.executor --project m-mapping-gen-2026 --step setup-raw
    python -m synthetic_data.executor --project m-mapping-gen-2026 --step load-raw
    python -m synthetic_data.executor --project m-mapping-gen-2026 --step setup-cdl
    python -m synthetic_data.executor --project m-mapping-gen-2026 --step transform-cdl
"""

import argparse
import os
import sys
import time
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: google-cloud-bigquery not installed.")
    print("Run: pip install google-cloud-bigquery")
    sys.exit(1)


SQL_DIR = Path(__file__).parent / "sql"
DATA_DIR = Path(__file__).parent / "output" / "raw"

LOCATION = "us-central1"

RAW_TABLES = [
    "raw_company",
    "raw_customer",
    "raw_product",
    "raw_price",
    "raw_cost",
    "raw_order_line",
    "raw_return",
    "raw_demand_forecast",
    "raw_sales_forecast",
]


def run_sql_file(client, project, filepath):
    """Execute a SQL file, splitting on semicolons for multi-statement files."""
    print(f"\n  Executing: {filepath.name}")
    sql = filepath.read_text()

    # Replace project placeholder if present
    sql = sql.replace("{{ project }}", project)

    # Split into individual statements
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for i, stmt in enumerate(statements, 1):
        # Skip comments-only blocks
        lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue

        # Extract a short description from the first comment
        desc = ""
        for l in stmt.split("\n"):
            if l.strip().startswith("--"):
                desc = l.strip().lstrip("- ").strip()
                break

        try:
            job = client.query(stmt + ";")
            job.result()  # Wait for completion
            print(f"    [{i}/{len(statements)}] OK: {desc or 'statement'}")
        except Exception as e:
            print(f"    [{i}/{len(statements)}] ERROR: {desc or 'statement'}")
            print(f"      {e}")


def setup_raw(client, project):
    """Step 1: Create RAW dataset and tables."""
    print("\n" + "=" * 60)
    print("STEP 1: Setting up RAW layer")
    print("=" * 60)

    ddl_file = SQL_DIR / "ddl" / "01_raw_tables.sql"
    if not ddl_file.exists():
        print(f"  ERROR: DDL file not found: {ddl_file}")
        return False

    run_sql_file(client, project, ddl_file)
    return True


def load_raw(client, project):
    """Step 2: Load JSONL data into RAW tables."""
    print("\n" + "=" * 60)
    print("STEP 2: Loading data into RAW tables")
    print("=" * 60)

    dataset_ref = f"{project}.Src_NovaStar"

    for table_name in RAW_TABLES:
        jsonl_file = DATA_DIR / f"{table_name}.jsonl"
        if not jsonl_file.exists():
            print(f"  SKIP: {jsonl_file} not found (run generate.py --format jsonl first)")
            continue

        table_ref = f"{dataset_ref}.{table_name}"
        print(f"\n  Loading {table_name}...")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
        )

        with open(jsonl_file, "rb") as f:
            job = client.load_table_from_file(f, table_ref, job_config=job_config)

        job.result()  # Wait for completion

        table = client.get_table(table_ref)
        print(f"    Loaded {table.num_rows:,} rows into {table_name}")

    return True


def setup_cdl(client, project):
    """Step 3: Create CDL dataset and tables."""
    print("\n" + "=" * 60)
    print("STEP 3: Setting up CDL layer")
    print("=" * 60)

    ddl_file = SQL_DIR / "ddl" / "02_cdl_tables.sql"
    if not ddl_file.exists():
        print(f"  ERROR: DDL file not found: {ddl_file}")
        return False

    run_sql_file(client, project, ddl_file)
    return True


def transform_cdl(client, project):
    """Step 4: Run RAW → CDL transformations."""
    print("\n" + "=" * 60)
    print("STEP 4: Running RAW → CDL transformations")
    print("=" * 60)

    transform_file = SQL_DIR / "transforms" / "01_raw_to_cdl.sql"
    if not transform_file.exists():
        print(f"  ERROR: Transform file not found: {transform_file}")
        return False

    run_sql_file(client, project, transform_file)
    return True


def verify(client, project):
    """Verify data counts in all tables."""
    print("\n" + "=" * 60)
    print("VERIFICATION: Row counts")
    print("=" * 60)

    datasets = {
        "Src_NovaStar": RAW_TABLES,
        "CDL_NovaStar": [
            "cdl_dim_company", "cdl_dim_customer", "cdl_dim_product",
            "cdl_dim_selling_method", "cdl_dim_calendar",
            "cdl_fact_sales", "cdl_fact_returns",
            "cdl_fact_product_price", "cdl_fact_product_cost",
            "cdl_fact_demand_forecast", "cdl_fact_sales_forecast",
        ],
    }

    for dataset, tables in datasets.items():
        print(f"\n  {dataset}:")
        for table_name in tables:
            try:
                table_ref = f"{project}.{dataset}.{table_name}"
                table = client.get_table(table_ref)
                print(f"    {table_name}: {table.num_rows:,} rows")
            except Exception:
                print(f"    {table_name}: NOT FOUND")

    return True


def main():
    parser = argparse.ArgumentParser(description="Execute NovaStar data pipeline")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--step", choices=["setup-raw", "load-raw", "setup-cdl", "transform-cdl", "verify", "all"],
                        default="all", help="Pipeline step to run (default: all)")
    parser.add_argument("--location", default=LOCATION, help="BigQuery location")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project, location=args.location)

    print("=" * 60)
    print("NovaStar Brands Corp - Pipeline Executor")
    print(f"Project: {args.project}")
    print(f"Location: {args.location}")
    print(f"Step: {args.step}")
    print("=" * 60)

    start = time.time()
    steps = {
        "setup-raw": lambda: setup_raw(client, args.project),
        "load-raw": lambda: load_raw(client, args.project),
        "setup-cdl": lambda: setup_cdl(client, args.project),
        "transform-cdl": lambda: transform_cdl(client, args.project),
        "verify": lambda: verify(client, args.project),
    }

    if args.step == "all":
        for name, fn in steps.items():
            if not fn():
                print(f"\n  FAILED at step: {name}")
                sys.exit(1)
    else:
        if not steps[args.step]():
            sys.exit(1)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Pipeline completed in {elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
