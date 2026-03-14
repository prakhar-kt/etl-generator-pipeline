"""
Synthetic Data Generator for NovaStar Brands Corp.
A fictional consumer goods company with subsidiaries across regions.

Generates RAW → CDL → BL layer data for:
- Sales transactions (invoiced, bookings, returns)
- Demand planning forecasts
- Sales forecasts
- Product, Company, Calendar, Customer dimensions

Usage:
    python -m synthetic_data.generate --format csv --output-dir synthetic_data/output
    python -m synthetic_data.generate --format bq-sql --output-dir synthetic_data/output
"""

import argparse
import csv
import hashlib
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# ─── Fictional Company: NovaStar Brands Corp ───────────────────────────────
# A global consumer goods company with toys, games, and entertainment products.

COMPANIES = [
    {"code": "NSG", "name": "NovaStar Germany", "region": "EMEA", "currency": "EUR", "geo_security": "EMEA"},
    {"code": "NSU", "name": "NovaStar USA", "region": "AMERICAS", "currency": "USD", "geo_security": "AMER"},
    {"code": "NSK", "name": "NovaStar UK", "region": "EMEA", "currency": "GBP", "geo_security": "EMEA"},
    {"code": "NSJ", "name": "NovaStar Japan", "region": "APAC", "currency": "JPY", "geo_security": "APAC"},
    {"code": "NSA", "name": "NovaStar Australia", "region": "APAC", "currency": "AUD", "geo_security": "APAC"},
    {"code": "NSF", "name": "NovaStar France", "region": "EMEA", "currency": "EUR", "geo_security": "EMEA"},
    {"code": "NSB", "name": "NovaStar Brazil", "region": "LATAM", "currency": "BRL", "geo_security": "LATM"},
    {"code": "NSC", "name": "NovaStar Canada", "region": "AMERICAS", "currency": "CAD", "geo_security": "AMER"},
    {"code": "NSM", "name": "NovaStar Mexico", "region": "LATAM", "currency": "MXN", "geo_security": "LATM"},
    {"code": "NSI", "name": "NovaStar India", "region": "APAC", "currency": "INR", "geo_security": "APAC"},
]

SELLING_METHODS = [
    {"code": "100", "name": "Wholesale"},
    {"code": "200", "name": "Direct-to-Consumer"},
    {"code": "300", "name": "E-Commerce"},
    {"code": "400", "name": "Marketplace"},
]

# Product lines for a toy/entertainment company
PRODUCT_LINES = [
    {"prefix": "ACT", "category": "Action Figures", "brand": "HeroForce"},
    {"prefix": "DLL", "category": "Fashion Dolls", "brand": "DreamLine"},
    {"prefix": "VHC", "category": "Vehicles", "brand": "TurboTrack"},
    {"prefix": "BRD", "category": "Board Games", "brand": "GameVault"},
    {"prefix": "PLH", "category": "Plush Toys", "brand": "CuddlePals"},
    {"prefix": "BLK", "category": "Building Blocks", "brand": "BlockCraft"},
    {"prefix": "EDU", "category": "Educational", "brand": "BrightMinds"},
    {"prefix": "OUT", "category": "Outdoor Play", "brand": "AdventureZone"},
]

PRODUCTS = []
for line in PRODUCT_LINES:
    for i in range(1, 16):  # 15 products per line = 120 total
        toy_no = f"{line['prefix']}{str(i).zfill(2)}"
        dash_codes = [str(random.randint(0, 5)) for _ in range(random.randint(1, 3))]
        for dash in dash_codes:
            PRODUCTS.append({
                "toy_no": toy_no,
                "dash_code": dash,
                "product_name": f"{line['brand']} {line['category']} #{i}-{dash}",
                "category": line["category"],
                "brand": line["brand"],
                "list_price": round(random.uniform(5.99, 89.99), 2),
                "est_invoice_price": round(random.uniform(3.99, 69.99), 2),
                "ex_factory_cost": round(random.uniform(1.50, 25.00), 2),
                "ww_cost": round(random.uniform(2.00, 30.00), 2),
            })

CUSTOMERS = []
for i in range(1, 51):
    CUSTOMERS.append({
        "code": f"CUST{str(i).zfill(4)}",
        "name": f"Retailer {random.choice(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta'])} {i}",
        "type": random.choice(["RETAIL", "WHOLESALE", "ONLINE", "DISTRIBUTOR"]),
    })

# Calendar: 2024 and 2025
def generate_calendar():
    """Generate weekly calendar entries for 2024-2025."""
    cal = []
    start = date(2024, 1, 1)
    # Find first Monday
    while start.weekday() != 0:
        start += timedelta(days=1)

    week_id = 1
    while start <= date(2025, 12, 31):
        week_end = start + timedelta(days=6)
        cal.append({
            "key_calendar": farm_fingerprint(f"{start.year}{start.isoformat()}"),
            "week_start_date": start,
            "week_end_date": week_end,
            "year": start.year,
            "month": start.month,
            "quarter": (start.month - 1) // 3 + 1,
            "week_of_year": week_id if start.year == 2024 else week_id - 52,
            "fiscal_year": start.year,
        })
        start += timedelta(days=7)
        week_id += 1
    return cal


def farm_fingerprint(value: str) -> int:
    """Simulate BigQuery's FARM_FINGERPRINT with a deterministic hash."""
    h = hashlib.md5(value.encode()).hexdigest()
    return int(h[:16], 16) - (1 << 63)  # Signed 64-bit


# ─── RAW Layer Tables ──────────────────────────────────────────────────────

def generate_raw_orders(calendar):
    """Generate raw order/transaction records."""
    rows = []
    order_id = 10000

    for week in calendar:
        for company in COMPANIES:
            for sm in SELLING_METHODS:
                # Number of orders varies by company size and selling method
                n_orders = random.randint(5, 30)
                for _ in range(n_orders):
                    order_id += 1
                    tx_date = week["week_start_date"] + timedelta(days=random.randint(0, 6))
                    if tx_date > date(2025, 12, 31):
                        continue
                    customer = random.choice(CUSTOMERS)
                    n_lines = random.randint(1, 8)

                    for line_no in range(1, n_lines + 1):
                        product = random.choice(PRODUCTS)
                        qty = random.randint(10, 5000)
                        price = product["list_price"]
                        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
                        selling_price = round(price * (1 - discount_pct / 100), 2)

                        rows.append({
                            "ORDER_ID": f"ORD-{order_id}",
                            "ORDER_LINE_NO": line_no,
                            "TRANSACTION_DATE": tx_date.isoformat(),
                            "COMPANY_CODE": company["code"],
                            "SELLING_METHOD_CODE": sm["code"],
                            "CUSTOMER_CODE": customer["code"],
                            "TOY_NO": product["toy_no"],
                            "DASH_CODE": product["dash_code"],
                            "ORDER_STATUS": random.choice(["SHIPPED", "SHIPPED", "SHIPPED", "INVOICED", "INVOICED"]),
                            "ORDER_TYPE": random.choice(["STANDARD", "STANDARD", "RUSH", "BACKORDER"]),
                            "ORDER_QTY": qty,
                            "LIST_PRICE": price,
                            "SELLING_PRICE": selling_price,
                            "DISCOUNT_PCT": discount_pct,
                            "ORDER_VALUE": round(qty * selling_price, 2),
                            "ORDER_LIST_VALUE": round(qty * price, 2),
                            "CURRENCY_CODE": company["currency"],
                            "LOAD_DATE": datetime.now().isoformat(),
                        })
    return rows


def generate_raw_returns(raw_orders):
    """Generate return records from a subset of orders."""
    rows = []
    # ~5% of orders get returns
    returnable = random.sample(raw_orders, k=min(len(raw_orders) // 20, 5000))

    for order in returnable:
        return_qty = random.randint(1, max(1, order["ORDER_QTY"] // 4))
        return_date = date.fromisoformat(order["TRANSACTION_DATE"]) + timedelta(days=random.randint(7, 60))
        if return_date > date(2025, 12, 31):
            continue
        rows.append({
            "RETURN_ID": f"RET-{order['ORDER_ID'].split('-')[1]}",
            "ORDER_ID": order["ORDER_ID"],
            "ORDER_LINE_NO": order["ORDER_LINE_NO"],
            "RETURN_DATE": return_date.isoformat(),
            "COMPANY_CODE": order["COMPANY_CODE"],
            "TOY_NO": order["TOY_NO"],
            "DASH_CODE": order["DASH_CODE"],
            "RETURN_QTY": -return_qty,
            "RETURN_VALUE": round(-return_qty * order["SELLING_PRICE"], 2),
            "RETURN_REASON": random.choice(["DEFECTIVE", "WRONG_ITEM", "CUSTOMER_CHANGE", "DAMAGED_SHIPPING"]),
            "CURRENCY_CODE": order["CURRENCY_CODE"],
            "LOAD_DATE": datetime.now().isoformat(),
        })
    return rows


def generate_raw_demand_forecasts(calendar):
    """Generate demand planning forecast records."""
    rows = []
    # Monthly snapshots (AS_OF_DATE = last day of prior month)
    as_of_dates = []
    for year in [2024, 2025]:
        for month in range(1, 13):
            last_day = date(year, month, 1) + timedelta(days=31)
            last_day = last_day.replace(day=1) - timedelta(days=1)
            as_of_dates.append(last_day)

    for as_of in as_of_dates:
        for company in COMPANIES:
            for sm in SELLING_METHODS[:2]:  # Only wholesale and DTC
                # Forecast for next 12 months
                for month_offset in range(0, 12):
                    forecast_month = as_of + timedelta(days=30 * month_offset)
                    if forecast_month.year > 2025:
                        continue

                    for product in random.sample(PRODUCTS, k=min(30, len(PRODUCTS))):
                        base_qty = random.randint(100, 50000)
                        # Seasonality: higher in Q4
                        if forecast_month.month in [10, 11, 12]:
                            base_qty = int(base_qty * 1.8)
                        elif forecast_month.month in [6, 7]:
                            base_qty = int(base_qty * 1.3)

                        rows.append({
                            "FORECAST_ID": f"DF-{len(rows) + 1}",
                            "AS_OF_DATE": as_of.isoformat(),
                            "FORECAST_DATE": forecast_month.replace(day=1).isoformat(),
                            "COMPANY_CODE": company["code"],
                            "SELLING_METHOD_CODE": sm["code"],
                            "TOY_NO": product["toy_no"],
                            "DASH_CODE": product["dash_code"],
                            "DEMAND_PLAN_QTY": base_qty,
                            "DEMAND_PLAN_DRAFT_QTY": int(base_qty * random.uniform(0.9, 1.1)),
                            "LINE_OF_BUSINESS_CODE": "G",
                            "CURRENCY_CODE": company["currency"],
                            "LOAD_DATE": datetime.now().isoformat(),
                        })
    return rows


def generate_raw_sales_forecasts(calendar):
    """Generate sales forecast records."""
    rows = []
    as_of_dates = []
    for year in [2024, 2025]:
        for month in range(1, 13):
            last_day = date(year, month, 1) + timedelta(days=31)
            last_day = last_day.replace(day=1) - timedelta(days=1)
            as_of_dates.append(last_day)

    for as_of in as_of_dates:
        for company in COMPANIES:
            for sm in SELLING_METHODS[:2]:
                for month_offset in range(0, 6):
                    forecast_month = as_of + timedelta(days=30 * month_offset)
                    if forecast_month.year > 2025:
                        continue

                    for product in random.sample(PRODUCTS, k=min(20, len(PRODUCTS))):
                        base_qty = random.randint(50, 30000)
                        if forecast_month.month in [10, 11, 12]:
                            base_qty = int(base_qty * 2.0)

                        rows.append({
                            "FORECAST_ID": f"SF-{len(rows) + 1}",
                            "AS_OF_DATE": as_of.isoformat(),
                            "FORECAST_DATE": forecast_month.replace(day=1).isoformat(),
                            "COMPANY_CODE": company["code"],
                            "SELLING_METHOD_CODE": sm["code"],
                            "TOY_NO": product["toy_no"],
                            "DASH_CODE": product["dash_code"],
                            "SALES_FORECAST_QTY": base_qty,
                            "SALES_FORECAST_STAGED_QTY": int(base_qty * random.uniform(0.85, 1.05)),
                            "CURRENCY_CODE": company["currency"],
                            "LOAD_DATE": datetime.now().isoformat(),
                        })
    return rows


def generate_raw_products():
    """Generate raw product master data."""
    rows = []
    for p in PRODUCTS:
        rows.append({
            "TOY_NO": p["toy_no"],
            "DASH_CODE": p["dash_code"],
            "PRODUCT_NAME": p["product_name"],
            "CATEGORY": p["category"],
            "BRAND": p["brand"],
            "STATUS": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DISCONTINUED"]),
            "LAUNCH_DATE": date(random.randint(2020, 2024), random.randint(1, 12), 1).isoformat(),
            "WEIGHT_KG": round(random.uniform(0.1, 5.0), 2),
            "LOAD_DATE": datetime.now().isoformat(),
        })
    return rows


def generate_raw_prices():
    """Generate raw product price records."""
    rows = []
    for p in PRODUCTS:
        for year in [2024, 2025]:
            for price_type, price_val in [
                ("LIST", p["list_price"]),
                ("EST_INVOICE", p["est_invoice_price"]),
            ]:
                # Slight year-over-year increase
                adj = 1.0 if year == 2024 else random.uniform(1.02, 1.08)
                rows.append({
                    "TOY_NO": p["toy_no"],
                    "DASH_CODE": p["dash_code"],
                    "PRICE_TYPE": price_type,
                    "PRICE": round(price_val * adj, 2),
                    "CURRENCY_CODE": "USD",
                    "EFFECTIVE_DATE": date(year, 1, 1).isoformat(),
                    "EXPIRY_DATE": date(year, 12, 31).isoformat(),
                    "LOAD_DATE": datetime.now().isoformat(),
                })
    return rows


def generate_raw_costs():
    """Generate raw product cost records."""
    rows = []
    for p in PRODUCTS:
        for year in [2024, 2025]:
            for cost_type, cost_val in [
                ("EX-FACTORY", p["ex_factory_cost"]),
                ("WORLDWIDE", p["ww_cost"]),
            ]:
                adj = 1.0 if year == 2024 else random.uniform(1.01, 1.06)
                rows.append({
                    "TOY_NO": p["toy_no"],
                    "DASH_CODE": p["dash_code"],
                    "COST_TYPE": cost_type,
                    "COST": round(cost_val * adj, 2),
                    "CURRENCY_CODE": "USD",
                    "EFFECTIVE_DATE": date(year, 1, 1).isoformat(),
                    "EXPIRY_DATE": date(year, 12, 31).isoformat(),
                    "LOAD_DATE": datetime.now().isoformat(),
                })
    return rows


def generate_raw_companies():
    """Generate raw company master data."""
    rows = []
    for c in COMPANIES:
        rows.append({
            "COMPANY_CODE": c["code"],
            "COMPANY_NAME": c["name"],
            "REGION": c["region"],
            "CURRENCY_CODE": c["currency"],
            "GEOGRAPHIC_SECURITY_CODE": c["geo_security"],
            "COUNTRY": c["name"].split()[-1],
            "STATUS": "ACTIVE",
            "LOAD_DATE": datetime.now().isoformat(),
        })
    return rows


def generate_raw_customers():
    """Generate raw customer master data."""
    rows = []
    for c in CUSTOMERS:
        rows.append({
            "CUSTOMER_CODE": c["code"],
            "CUSTOMER_NAME": c["name"],
            "CUSTOMER_TYPE": c["type"],
            "STATUS": "ACTIVE",
            "CREDIT_LIMIT": random.randint(50000, 5000000),
            "PAYMENT_TERMS": random.choice(["NET30", "NET60", "NET90"]),
            "LOAD_DATE": datetime.now().isoformat(),
        })
    return rows


# ─── Output Helpers ────────────────────────────────────────────────────────

def write_csv(rows, filepath):
    """Write rows to CSV file."""
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows):,} rows → {filepath}")


def write_bq_sql(rows, table_name, dataset, filepath):
    """Write BigQuery-compatible CREATE TABLE + INSERT statements."""
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Infer schema from first row
    type_map = {}
    for k, v in rows[0].items():
        if isinstance(v, int):
            type_map[k] = "INT64"
        elif isinstance(v, float):
            type_map[k] = "NUMERIC"
        elif isinstance(v, date) and not isinstance(v, datetime):
            type_map[k] = "DATE"
        else:
            val = str(v)
            if len(val) == 10 and val[4] == "-" and val[7] == "-":
                type_map[k] = "DATE"
            elif "T" in val and len(val) > 15:
                type_map[k] = "TIMESTAMP"
            else:
                type_map[k] = "STRING"

    with open(filepath, "w") as f:
        # CREATE TABLE
        f.write(f"-- Auto-generated synthetic data for {dataset}.{table_name}\n")
        f.write(f"-- NovaStar Brands Corp (fictional)\n\n")
        f.write(f"CREATE TABLE IF NOT EXISTS `{{{{ project }}}}.{dataset}.{table_name}` (\n")
        cols = list(rows[0].keys())
        for i, col in enumerate(cols):
            comma = "," if i < len(cols) - 1 else ""
            f.write(f"  {col} {type_map[col]}{comma}\n")
        f.write(f");\n\n")

        # INSERT in batches of 500
        batch_size = 500
        for batch_start in range(0, len(rows), batch_size):
            batch = rows[batch_start:batch_start + batch_size]
            f.write(f"INSERT INTO `{{{{ project }}}}.{dataset}.{table_name}` ({', '.join(cols)}) VALUES\n")
            for j, row in enumerate(batch):
                vals = []
                for col in cols:
                    v = row[col]
                    if v is None:
                        vals.append("NULL")
                    elif type_map[col] in ("INT64",):
                        vals.append(str(v))
                    elif type_map[col] in ("NUMERIC",):
                        vals.append(str(v))
                    elif type_map[col] == "DATE":
                        vals.append(f"DATE '{v}'")
                    elif type_map[col] == "TIMESTAMP":
                        vals.append(f"TIMESTAMP '{v}'")
                    else:
                        vals.append(f"'{str(v).replace(chr(39), chr(39)+chr(39))}'")
                comma = "," if j < len(batch) - 1 else ";"
                f.write(f"  ({', '.join(vals)}){comma}\n")
            f.write("\n")

    print(f"  Written {len(rows):,} rows → {filepath}")


def write_json(rows, filepath):
    """Write rows as newline-delimited JSON (for BQ load)."""
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        for row in rows:
            # Convert date objects to strings
            clean = {}
            for k, v in row.items():
                if isinstance(v, (date, datetime)):
                    clean[k] = v.isoformat()
                else:
                    clean[k] = v
            f.write(json.dumps(clean) + "\n")
    print(f"  Written {len(rows):,} rows → {filepath}")


# ─── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic data for NovaStar Brands Corp")
    parser.add_argument("--format", choices=["csv", "bq-sql", "jsonl"], default="csv",
                        help="Output format (default: csv)")
    parser.add_argument("--output-dir", default="synthetic_data/output",
                        help="Output directory")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--scale", choices=["small", "medium", "large"], default="small",
                        help="Data scale: small (~10K rows), medium (~100K), large (~1M)")
    args = parser.parse_args()

    random.seed(args.seed)
    out = Path(args.output_dir)

    print("=" * 60)
    print("NovaStar Brands Corp - Synthetic Data Generator")
    print("=" * 60)

    # Generate calendar
    print("\nGenerating calendar...")
    calendar = generate_calendar()

    # Scale control: limit weeks processed for orders
    if args.scale == "small":
        order_calendar = random.sample(calendar, k=min(12, len(calendar)))
    elif args.scale == "medium":
        order_calendar = random.sample(calendar, k=min(52, len(calendar)))
    else:
        order_calendar = calendar

    # ─── RAW Layer ──────────────────────────────────────────
    print("\n--- RAW Layer ---")

    print("Generating raw companies...")
    raw_companies = generate_raw_companies()

    print("Generating raw customers...")
    raw_customers = generate_raw_customers()

    print("Generating raw products...")
    raw_products = generate_raw_products()

    print("Generating raw prices...")
    raw_prices = generate_raw_prices()

    print("Generating raw costs...")
    raw_costs = generate_raw_costs()

    print("Generating raw orders (this may take a moment)...")
    raw_orders = generate_raw_orders(order_calendar)

    print("Generating raw returns...")
    raw_returns = generate_raw_returns(raw_orders)

    print("Generating raw demand forecasts...")
    raw_demand = generate_raw_demand_forecasts(calendar)

    print("Generating raw sales forecasts...")
    raw_sales_fc = generate_raw_sales_forecasts(calendar)

    # ─── Write Output ──────────────────────────────────────
    raw_tables = {
        "raw_company": ("Src_NovaStar", raw_companies),
        "raw_customer": ("Src_NovaStar", raw_customers),
        "raw_product": ("Src_NovaStar", raw_products),
        "raw_price": ("Src_NovaStar", raw_prices),
        "raw_cost": ("Src_NovaStar", raw_costs),
        "raw_order_line": ("Src_NovaStar", raw_orders),
        "raw_return": ("Src_NovaStar", raw_returns),
        "raw_demand_forecast": ("Src_NovaStar", raw_demand),
        "raw_sales_forecast": ("Src_NovaStar", raw_sales_fc),
    }

    for table_name, (dataset, rows) in raw_tables.items():
        if args.format == "csv":
            write_csv(rows, str(out / "raw" / f"{table_name}.csv"))
        elif args.format == "bq-sql":
            write_bq_sql(rows, table_name, dataset, str(out / "raw" / f"{table_name}.sql"))
        elif args.format == "jsonl":
            write_json(rows, str(out / "raw" / f"{table_name}.jsonl"))

    # ─── Summary ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Summary:")
    total = sum(len(rows) for _, rows in raw_tables.values())
    print(f"  Total RAW rows: {total:,}")
    for name, (_, rows) in raw_tables.items():
        print(f"    {name}: {len(rows):,} rows")
    print(f"\n  Output directory: {out}")
    print(f"  Format: {args.format}")
    print("=" * 60)


if __name__ == "__main__":
    main()
