"""
Generates synthetic orders/customers CSV+JSON files to use as the Phase 1
batch source, so you don't need a real production database to get started.

Writes locally under ./seed_data/<source>/, one timestamped file per run
(files are never overwritten). Running this more than once is intentional:
customer_id intentionally repeats across runs (same 1..NUM_CUSTOMERS range),
giving the silver-layer dedup logic (stg_customers.sql's `qualify
row_number()...`) real duplicate rows to clean up, and giving bronze more
than one Delta version to demonstrate time travel with.

For the cloud path, upload these files to your S3 raw/ prefix instead (or
point Auto Loader at them for early testing) -- see docs/build-plan.md.
"""
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

OUT_DIR = "seed_data"
NUM_CUSTOMERS = 500
NUM_ORDERS = 5000
REGIONS = ["us-east", "us-west", "eu-west", "apac"]
STATUSES = ["completed", "cancelled", "pending", "refunded"]


def seed_customers() -> list[dict]:
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 700))
        customers.append(
            {
                "customer_id": i,
                "email": f"user{i}@example.com",
                "signup_date": signup.date().isoformat(),
                "region": random.choice(REGIONS),
                "updated_at": signup.isoformat(),
            }
        )
    return customers


def seed_orders(customer_ids: list[int]) -> list[dict]:
    orders = []
    for _ in range(NUM_ORDERS):
        created = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 90))
        orders.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_id": random.choice(customer_ids),
                "order_amount": round(random.uniform(10, 500), 2),
                "order_status": random.choices(STATUSES, weights=[70, 10, 10, 10])[0],
                "created_at": created.isoformat(),
                "updated_at": created.isoformat(),
            }
        )
    return orders


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

    customers_dir = os.path.join(OUT_DIR, "customers")
    orders_dir = os.path.join(OUT_DIR, "orders")
    os.makedirs(customers_dir, exist_ok=True)
    os.makedirs(orders_dir, exist_ok=True)

    customers = seed_customers()
    customers_path = os.path.join(customers_dir, f"customers_{timestamp}.csv")
    with open(customers_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    orders = seed_orders([c["customer_id"] for c in customers])
    orders_path = os.path.join(orders_dir, f"orders_{timestamp}.json")
    with open(orders_path, "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    print(f"Wrote {len(customers)} customers to {customers_path}")
    print(f"Wrote {len(orders)} orders to {orders_path}")


if __name__ == "__main__":
    main()
