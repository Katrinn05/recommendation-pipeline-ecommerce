#!/usr/bin/env python3
"""
Streaming feature engineering pipeline for Stage 2.

This script consumes Avro-encoded events from Kafka topics ("product-clicks",
"cart-adds" and "purchases") and constructs per-user aggregated features.
For each user and each calendar day the pipeline counts the number of click,
cart and purchase events.  The aggregated features are periodically flushed to
Parquet files partitioned by date so that they may serve as the offline source
for a feature store (e.g., Feast).

"""
import json
import os
import argparse
import logging
import signal
import time
from io import BytesIO
from datetime import datetime, timezone
from collections import defaultdict

from kafka import KafkaConsumer
from fastavro import parse_schema, schemaless_reader

def parse_args():
    p = argparse.ArgumentParser(description="Streaming feature engineering")
    p.add_argument("--bootstrap-server", default="localhost:9092")
    p.add_argument("--output-dir", default="data/offline")
    p.add_argument("--flush-every", type=int, default=10_000)
    p.add_argument("--flush-seconds", type=int, default=60,
               help="Flush aggregates at least this often (seconds).")
    p.add_argument("--max-messages", type=int, default=0, help="0 means unlimited")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

# Mapping of Kafka topics to their respective Avro schema files.
SCHEMA_PATHS = {
    "product-clicks": "schemas/product_clicks.avsc",
    "cart-adds": "schemas/cart_adds.avsc",
    "purchases": "schemas/purchases.avsc",
}

def load_schemas():
    """Load and parse Avro schemas for all configured topics."""
    schemas: dict[str, dict] = {}
    for topic, path in SCHEMA_PATHS.items():
        with open(path, "r", encoding="utf-8") as f:
            schemas[topic] = parse_schema(json.load(f))
    return schemas

def decode_avro(value_bytes: bytes, schema: dict) -> dict:
    """Decode Avro binary bytes into a Python dictionary."""
    return schemaless_reader(BytesIO(value_bytes), schema)

def flush(aggregates: dict, output_root: str = "data/offline") -> None:
    """
    Persist the aggregated feature dictionaries to partitioned Parquet files.

    Each unique ``event_date`` will be written to its own subdirectory
    ``partition_date=<YYYY-MM-DD>`` under ``output_root``.  The resulting
    Parquet file (``user_daily_features.parquet``) contains one row per user.
    """
    import pandas as pd

    rows: list[dict] = []
    for (user_id, date_str), counts in aggregates.items():
        distinct = len(counts["products"]) if isinstance(counts.get("products"), set) else None
        rows.append({
            "user_id": user_id,
            "event_date": date_str,
            "click_count_24h": counts["clicks"],
            "cart_add_count_24h": counts["carts"],
            "purchase_count_24h": counts["purchases"],
            "distinct_products_24h": distinct,
            "event_timestamp": datetime.now(timezone.utc),
            "ingestion_time": datetime.now(timezone.utc),
        })
    if not rows:
        return

    df = pd.DataFrame(rows)
    for date_str, group in df.groupby("event_date"):
        partition_path = os.path.join(output_root, f"partition_date={date_str}")
        os.makedirs(partition_path, exist_ok=True)
        file_path = os.path.join(partition_path, "user_daily_features.parquet")
        group.to_parquet(
            file_path,
            index=False,
            engine="pyarrow",
            compression="snappy"
        )

def main() -> None:
    last_flush = time.monotonic()
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    schemas = load_schemas()

    consumer = KafkaConsumer(
        *SCHEMA_PATHS.keys(),
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )

    stop = {"value": False}
    def _graceful(*_):
        stop["value"] = True
    signal.signal(signal.SIGINT, _graceful)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _graceful)

    aggregates = defaultdict(lambda: {
        "clicks": 0,
        "carts": 0,
        "purchases": 0,
        "products": set(),
    })
    message_counter = 0

    try:
        for msg in consumer:
            try:
                record = decode_avro(msg.value, schemas[msg.topic])
            except Exception as e:
                logging.warning("avro_decode_error topic=%s err=%s", msg.topic, e)
                continue
            
            user_id = record.get("user_id")
            if not user_id:
                logging.warning("missing_user_id topic=%s", msg.topic)
                continue

            ts_ms = record.get("timestamp") or msg.timestamp
            event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            date_str = event_time.strftime("%Y-%m-%d")
            key = (user_id, date_str)

            product_id = record.get("product_id")
            if product_id is not None:
                aggregates[key]["products"].add(product_id)

            if msg.topic == "product-clicks":
                aggregates[key]["clicks"] += 1
            elif msg.topic == "cart-adds":
                aggregates[key]["carts"] += 1
            elif msg.topic == "purchases":
                aggregates[key]["purchases"] += 1

            message_counter += 1
            should_flush = False
            if message_counter % args.flush_every == 0:
                should_flush = True
            if time.monotonic() - last_flush >= args.flush_seconds:
                should_flush = True

            if should_flush:
                flush(aggregates, output_root=args.output_dir)
                aggregates.clear()
                last_flush = time.monotonic()
                logging.info("flushed count=%d", message_counter)
    finally:
        flush(aggregates, output_root=args.output_dir)
        consumer.close()

if __name__ == "__main__":
    main()