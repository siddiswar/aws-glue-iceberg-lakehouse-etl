import os, csv, io, random
import boto3
from datetime import datetime, timezone
from decimal import Decimal

ddb = boto3.client("dynamodb")
dynamo = boto3.resource("dynamodb")
s3 = boto3.client("s3")

STATE_TABLE = os.environ.get("STATE_TABLE", "etl_generator_state")
DIM_TABLE = os.environ.get("DIM_TABLE", "etl_dim_store")
BUCKET = os.environ.get("RAW_BUCKET")

TS_FMT = "%Y-%m-%d %H:%M:%S"
dim_tbl = dynamo.Table(DIM_TABLE)


def put_csv_s3(key, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))


def allocate_seq(counter_pk, n):
    resp = ddb.update_item(
        TableName=STATE_TABLE,
        Key={"pk": {"S": counter_pk}},
        UpdateExpression="SET next_seq = if_not_exists(next_seq, :zero) + :n",
        ExpressionAttributeValues={":n": {"N": str(n)}, ":zero": {"N": "0"}},
        ReturnValues="UPDATED_NEW",
    )
    new_next = int(resp["Attributes"]["next_seq"]["N"])
    return new_next - n + 1


def to_ddb_safe(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: to_ddb_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_ddb_safe(v) for v in value]
    return value


def mark_run_started(dt):
    ddb.put_item(
        TableName=STATE_TABLE,
        Item={
            "pk": {"S": f"run#{dt}"},
            "dt": {"S": dt},
            "status": {"S": "STARTED"},
            "started_at": {"S": datetime.now(timezone.utc).strftime(TS_FMT)},
        },
        ConditionExpression="attribute_not_exists(pk)"
    )


def mark_run_completed(dt):
    ddb.update_item(
        TableName=STATE_TABLE,
        Key={"pk": {"S": f"run#{dt}"}},
        UpdateExpression="SET #s=:c, completed_at=:t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":c": {"S": "COMPLETED"},
            ":t": {"S": datetime.now(timezone.utc).strftime(TS_FMT)},
        },
    )


def scan_entity(entity):
    items = []
    resp = dim_tbl.scan(
        FilterExpression="entity = :e",
        ExpressionAttributeValues={":e": entity},
    )
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = dim_tbl.scan(
            FilterExpression="entity = :e",
            ExpressionAttributeValues={":e": entity},
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    return items


def upsert_dim_item(entity, id_, attrs):
    item = {"entity": entity, "id": id_, **attrs}
    dim_tbl.put_item(Item=to_ddb_safe(item))


def lambda_handler(event, context):
    dt = event["dt"]
    rng = random.Random(int(dt.replace("-", "")))

    if not BUCKET:
        raise ValueError("RAW_BUCKET environment variable is not set")

    # If DynamoDB says this day is already generated, we still need to ensure
    # the raw files exist in S3 (you may have emptied the bucket).
    try:
        mark_run_started(dt)
    except Exception:
        # Check whether today's raw files exist; if not, allow regeneration.
        expected_keys = [
            f"raw/customers/dt={dt}/customers.csv",
            f"raw/products/dt={dt}/products.csv",
            f"raw/orders/dt={dt}/orders.csv",
        ]

        missing = False
        for k in expected_keys:
            try:
                s3.head_object(Bucket=BUCKET, Key=k)
            except Exception:
                missing = True
                break

        if not missing:
            return {"status": "skipped", "reason": "already generated", "dt": dt, "bucket": BUCKET}

        # Regenerate: remove the run marker and re-create it.
        ddb.delete_item(
            TableName=STATE_TABLE,
            Key={"pk": {"S": f"run#{dt}"}},
        )
        mark_run_started(dt)

    customers = scan_entity("customer")
    products = scan_entity("product")

    if not customers:
        start = allocate_seq("counter#customers", 50)
        for i in range(start, start + 50):
            cid = f"C{i:06d}"
            upsert_dim_item("customer", cid, {
                "full_name": f"Customer {cid}",
                "email": f"{cid.lower()}@example.com",
                "city": rng.choice(["London", "Milton Keynes", "Manchester", "Birmingham"]),
                "updated_at": f"{dt} 00:00:00",
            })
        customers = scan_entity("customer")

    if not products:
        start = allocate_seq("counter#products", 20)
        for i in range(start, start + 20):
            pid = f"P{i:06d}"
            upsert_dim_item("product", pid, {
                "product_name": f"Product {pid}",
                "category": rng.choice(["Books", "Electronics", "Grocery", "Home"]),
                "list_price": round(rng.uniform(5, 200), 2),
                "updated_at": f"{dt} 00:00:00",
            })
        products = scan_entity("product")

    # SCD2-like changes
    for c in customers:
        if rng.random() < 0.08:
            cid = c["id"]
            upsert_dim_item("customer", cid, {
                "full_name": c.get("full_name", f"Customer {cid}"),
                "email": c.get("email", f"{cid.lower()}@example.com"),
                "city": rng.choice(["London", "Leeds", "Bristol", "Edinburgh"]),
                "updated_at": f"{dt} 00:00:00",
            })

    for p in products:
        if rng.random() < 0.06:
            pid = p["id"]
            upsert_dim_item("product", pid, {
                "product_name": p.get("product_name", f"Product {pid}"),
                "category": rng.choice(["Books", "Electronics", "Grocery", "Home"]),
                "list_price": round(rng.uniform(5, 200), 2),
                "updated_at": f"{dt} 00:00:00",
            })

    customers = scan_entity("customer")
    products = scan_entity("product")

    cust_rows = [[c["id"], c.get("full_name", ""), c.get("email", ""), c.get("city", ""), c.get("updated_at", "")] for c
                 in customers]
    prod_rows = [
        [p["id"], p.get("product_name", ""), p.get("category", ""), p.get("list_price", 0.0), p.get("updated_at", "")]
        for p in products]

    put_csv_s3(f"raw/customers/dt={dt}/customers.csv",
               ["customer_id", "full_name", "email", "city", "updated_at"], cust_rows)

    put_csv_s3(f"raw/products/dt={dt}/products.csv",
               ["product_id", "product_name", "category", "list_price", "updated_at"], prod_rows)

    price_by_pid = {p["id"]: float(p.get("list_price", 0.0)) for p in products}
    customer_ids = [c["id"] for c in customers]
    product_ids = [p["id"] for p in products]

    orders_per_day = 200
    start_seq = allocate_seq("counter#orders", orders_per_day)

    order_rows = []
    for seq in range(start_seq, start_seq + orders_per_day):
        oid = f"O{dt.replace('-', '')}-{seq:06d}"
        ts = f"{dt} {rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        cid = rng.choice(customer_ids)
        pid = rng.choice(product_ids)
        qty = rng.randint(1, 5)
        unit_price = price_by_pid.get(pid, round(rng.uniform(5, 200), 2))
        order_rows.append([oid, ts, cid, pid, qty, round(unit_price, 2)])

    put_csv_s3(f"raw/orders/dt={dt}/orders.csv",
               ["order_id", "order_ts", "customer_id", "product_id", "quantity", "unit_price"], order_rows)

    mark_run_completed(dt)
    return {"status": "ok", "dt": dt, "bucket": BUCKET,
            "counts": {"customers": len(customers), "products": len(products), "orders": orders_per_day}}
