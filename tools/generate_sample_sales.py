"""Generate data/sample_sales.csv: 100 synthetic rows, seed 42.

Pure Python, no Spark needed. Run from the repository root:

    python tools/generate_sample_sales.py

The output is invented test data for checking the schema and
the column order used by the notebooks. It is not real
customer or sales data, and its size says nothing about
performance at the 1,000,000-row configured scale.
"""

import csv
import random
from pathlib import Path

SEED = 42
ROWS = 100

REGIONS = ["North", "South", "East", "West"]
CHANNELS = ["Online", "Retail", "Corporate"]
STATUSES = ["Completed", "Returned", "Pending"]
PRODUCTS = [
    ("PROD101", "Laptop Pro X"),
    ("PROD102", "Wireless Mouse"),
    ("PROD103", "USB-C Hub"),
    ("PROD104", "Mechanical Keyboard"),
    ("PROD105", "4K Monitor"),
    ("PROD106", "Webcam HD"),
    ("PROD107", "Headphones Pro"),
    ("PROD108", "SSD 1TB"),
    ("PROD109", "Smart Speaker"),
    ("PROD110", "Tablet 10inch"),
]


def main() -> None:
    rng = random.Random(SEED)
    out_path = Path(__file__).resolve().parent.parent / "data" / "sample_sales.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "region", "product_id", "product_name",
            "sale_amount", "return_amount", "sale_date", "sales_rep_id",
            "channel", "status",
        ])
        for _ in range(ROWS):
            product_id, product_name = rng.choice(PRODUCTS)
            sale_amount = round(rng.uniform(10, 910), 2)
            return_amount = (
                round(sale_amount * rng.random(), 2)
                if rng.random() < 0.08 else 0.0
            )
            writer.writerow([
                f"CUST{rng.randint(1, 50000):06d}",
                rng.choice(REGIONS),
                product_id,
                product_name,
                f"{sale_amount:.2f}",
                f"{return_amount:.2f}",
                f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
                f"REP{rng.randint(1, 50):03d}",
                rng.choice(CHANNELS),
                rng.choice(STATUSES),
            ])

    print(f"Wrote {ROWS} rows to {out_path}")


if __name__ == "__main__":
    main()
