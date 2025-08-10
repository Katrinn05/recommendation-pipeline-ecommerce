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
from io import BytesIO
from datetime import datetime, timezone
from collections import defaultdict

from kafka import KafkaConsumer
from fastavro import parse_schema, schemaless_reader

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
        rows.append({
            "user_id": user_id,
            "event_date": date_str,
            "click_count_24h": counts["clicks"],
            "cart_add_count_24h": counts["carts"],
            "purchase_count_24h": counts["purchases"],
            # event_timestamp marks when the aggregation is computed; using UTC now
            "event_timestamp": datetime.now(timezone.utc),
            # ingestion_time marks when the record was written to storage
            "ingestion_time": datetime.now(timezone.utc),
        })
    if not rows:
        return

    df = pd.DataFrame(rows)
    for date_str, group in df.groupby("event_date"):
        partition_path = os.path.join(output_root, f"partition_date={date_str}")
        os.makedirs(partition_path, exist_ok=True)
        file_path = os.path.join(partition_path, "user_daily_features.parquet")
        # Write Parquet; for simplicity overwrite existing file
        group.to_parquet(
            file_path,
            index=False,
            engine="pyarrow",
            compression="snappy"
        )

def main() -> None:
    # Load schemas once at startup
    schemas = load_schemas()

    # Create a Kafka consumer for all topics.  auto_offset_reset="earliest"
    # allows replaying events from the beginning for offline backfilling.
    consumer = KafkaConsumer(
        *SCHEMA_PATHS.keys(),
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    # Aggregates keyed by (user_id, date_str)
    aggregates: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"clicks": 0, "carts": 0, "purchases": 0}
    )

    message_counter = 0
    try:
        for msg in consumer:
            schema = schemas[msg.topic]
            record = decode_avro(msg.value, schema)
            user_id: str = record["user_id"]
            # Convert millisecond timestamp to date string (YYYY-MM-DD)
            event_time = datetime.fromtimestamp(record["timestamp"] / 1000.0)
            date_str = event_time.strftime("%Y-%m-%d")
            key = (user_id, date_str)

            # Increment appropriate counter based on topic
            if msg.topic == "product-clicks":
                aggregates[key]["clicks"] += 1
            elif msg.topic == "cart-adds":
                aggregates[key]["carts"] += 1
            elif msg.topic == "purchases":
                aggregates[key]["purchases"] += 1

            message_counter += 1
            # Flush aggregated data to disk every 10 000 messages to limit memory usage
            if message_counter % 10000 == 0:
                flush(aggregates)
                aggregates.clear()

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        pass
    finally:
        flush(aggregates)
        consumer.close()

if __name__ == "__main__":
    main()