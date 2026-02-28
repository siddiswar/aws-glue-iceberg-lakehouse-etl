from __future__ import annotations


def raw_entity_path(bucket: str, entity: str, dt: str, filename: str) -> str:
    # entity like 'customers'/'products'/'orders'
    return f"s3://{bucket}/raw/{entity}/dt={dt}/{filename}"


def raw_customers_path(bucket: str, dt: str) -> str:
    return raw_entity_path(bucket, "customers", dt, "customers.csv")


def raw_products_path(bucket: str, dt: str) -> str:
    return raw_entity_path(bucket, "products", dt, "products.csv")


def raw_orders_path(bucket: str, dt: str) -> str:
    return raw_entity_path(bucket, "orders", dt, "orders.csv")
