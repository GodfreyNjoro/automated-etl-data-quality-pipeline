"""Generate synthetic multi-source retail data with realistic quality issues.

Four sources are produced, mirroring a typical retail data landscape:

    products.csv       - product catalog (CSV export)
    inventory.csv      - warehouse stock levels (simulated DB export)
    pos_transactions.json - point-of-sale transactions (simulated REST API)
    customers.json     - CRM customer records (simulated REST API)

Deliberate data-quality problems are injected so the quality framework,
quarantine flow and dashboard have something meaningful to surface:

    * missing values (nulls in required fields)
    * duplicate primary keys
    * out-of-range / negative numbers and price outliers
    * invalid categorical values
    * stale timestamps

The generator is deterministic (seeded) so runs are reproducible.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

CATEGORIES = ["Electronics", "Grocery", "Apparel", "Home", "Beauty", "Toys"]
STORES = ["STORE-001", "STORE-002", "STORE-003", "STORE-004"]
PAYMENT_METHODS = ["card", "cash", "mobile", "voucher"]


def _seed(seed: int) -> random.Random:
    return random.Random(seed)


def generate_products(rng: random.Random, n: int = 120) -> list[dict]:
    products = []
    for i in range(1, n + 1):
        category = rng.choice(CATEGORIES)
        price = round(rng.uniform(2.0, 500.0), 2)
        products.append(
            {
                "product_id": f"P{i:05d}",
                "product_name": f"{category} Item {i}",
                "category": category,
                "unit_price": price,
                "supplier_id": f"SUP-{rng.randint(1, 25):03d}",
                "active": rng.random() > 0.05,
            }
        )

    # --- Inject quality issues ---
    # Missing category on a few rows.
    for idx in rng.sample(range(n), 4):
        products[idx]["category"] = None
    # Invalid category value.
    products[rng.randrange(n)]["category"] = "Unknown"
    # Duplicate product_id.
    dup = dict(products[10])
    dup["product_name"] += " (dup)"
    products.append(dup)
    # Price outlier and a negative price.
    products[rng.randrange(n)]["unit_price"] = 999999.0
    products[rng.randrange(n)]["unit_price"] = -15.0
    # Missing unit price.
    products[rng.randrange(n)]["unit_price"] = None
    return products


def generate_inventory(rng: random.Random, products: list[dict]) -> list[dict]:
    inventory = []
    for p in products:
        for store in STORES:
            inventory.append(
                {
                    "product_id": p["product_id"],
                    "store_id": store,
                    "quantity_on_hand": rng.randint(0, 500),
                    "reorder_level": rng.randint(10, 50),
                    "last_counted": (
                        datetime.now(timezone.utc) - timedelta(days=rng.randint(0, 20))
                    ).strftime("%Y-%m-%d"),
                }
            )
    # --- Inject quality issues ---
    # Negative stock (impossible).
    for idx in rng.sample(range(len(inventory)), 6):
        inventory[idx]["quantity_on_hand"] = -rng.randint(1, 20)
    # Missing quantity.
    for idx in rng.sample(range(len(inventory)), 5):
        inventory[idx]["quantity_on_hand"] = None
    return inventory


def generate_pos_transactions(rng: random.Random, products: list[dict], n: int = 2000) -> dict:
    valid_ids = [p["product_id"] for p in products if p["product_id"]]
    transactions = []
    now = datetime.now(timezone.utc)
    for i in range(1, n + 1):
        ts = now - timedelta(minutes=rng.randint(0, 60 * 24 * 7))
        qty = rng.randint(1, 8)
        unit_price = round(rng.uniform(2.0, 500.0), 2)
        transactions.append(
            {
                "transaction_id": f"T{i:08d}",
                "product_id": rng.choice(valid_ids),
                "store_id": rng.choice(STORES),
                "quantity": qty,
                "unit_price": unit_price,
                "total_amount": round(qty * unit_price, 2),
                "payment_method": rng.choice(PAYMENT_METHODS),
                "transaction_ts": ts.isoformat(),
            }
        )

    # --- Inject quality issues ---
    # Duplicate transaction ids.
    for _ in range(8):
        transactions.append(dict(rng.choice(transactions)))
    # Missing product_id references.
    for idx in rng.sample(range(n), 10):
        transactions[idx]["product_id"] = None
    # Invalid payment method.
    for idx in rng.sample(range(n), 6):
        transactions[idx]["payment_method"] = "crypto"
    # Negative quantity / zero.
    for idx in rng.sample(range(n), 5):
        transactions[idx]["quantity"] = -rng.randint(1, 3)
    # total_amount inconsistent with qty*price handled as a business rule.
    for idx in rng.sample(range(n), 5):
        transactions[idx]["total_amount"] = 0.0
    # Wrap like a paginated REST payload.
    return {"data": transactions, "meta": {"count": len(transactions)}}


def generate_customers(rng: random.Random, n: int = 400) -> dict:
    customers = []
    signup_start = datetime.now(timezone.utc) - timedelta(days=900)
    for i in range(1, n + 1):
        signup = signup_start + timedelta(days=rng.randint(0, 900))
        customers.append(
            {
                "customer_id": f"C{i:06d}",
                "email": f"customer{i}@example.com",
                "full_name": f"Customer {i}",
                "loyalty_tier": rng.choice(["bronze", "silver", "gold", "platinum"]),
                "lifetime_value": round(rng.uniform(0, 15000), 2),
                "signup_date": signup.strftime("%Y-%m-%d"),
                "country": rng.choice(["US", "UK", "KE", "CA", "AU", "DE"]),
            }
        )

    # --- Inject quality issues ---
    # Missing emails.
    for idx in rng.sample(range(n), 12):
        customers[idx]["email"] = None
    # Duplicate customer id.
    customers.append(dict(customers[3]))
    # Invalid loyalty tier.
    for idx in rng.sample(range(n), 5):
        customers[idx]["loyalty_tier"] = "diamond"
    # Negative lifetime value.
    for idx in rng.sample(range(n), 4):
        customers[idx]["lifetime_value"] = -rng.uniform(1, 500)
    return {"data": customers, "meta": {"count": len(customers)}}


def _write_csv(path: Path, rows: list[dict]) -> None:
    import csv

    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_all(seed: int = 42, out_dir: Path | None = None) -> dict[str, Path]:
    out_dir = out_dir or DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = _seed(seed)

    products = generate_products(rng)
    inventory = generate_inventory(rng, products)
    transactions = generate_pos_transactions(rng, products)
    customers = generate_customers(rng)

    paths = {
        "products": out_dir / "products.csv",
        "inventory": out_dir / "inventory.csv",
        "pos_transactions": out_dir / "pos_transactions.json",
        "customers": out_dir / "customers.json",
    }
    _write_csv(paths["products"], products)
    _write_csv(paths["inventory"], inventory)
    paths["pos_transactions"].write_text(json.dumps(transactions, indent=2))
    paths["customers"].write_text(json.dumps(customers, indent=2))
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic retail data.")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    paths = generate_all(seed=args.seed)
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
