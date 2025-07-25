"""
Smoke-test consumer for Kafka events with Avro decoding.

Usage:
  python3 scripts/print_events.py \
    --topic cart-adds \
    --schema schemas/cart_adds.avsc \
    --bootstrap-server localhost:9092
"""
import argparse
import json
from io import BytesIO

from kafka import KafkaConsumer
from fastavro import parse_schema, schemaless_reader


def main():
    parser = argparse.ArgumentParser(
        description="Smokes-test Kafka topic by printing incoming Avro events."
    )
    parser.add_argument(
        "-t", "--topic",
        default="cart-adds",
        help="Kafka topic to consume from"
    )
    parser.add_argument(
        "-s", "--schema",
        default="schemas/cart_adds.avsc",
        help="Path to Avro schema file (.avsc)"
    )
    parser.add_argument(
        "-b", "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address"
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=50000,
        help="exit if no messages arrive in this many ms")
    
    parser.add_argument(
        "--max-messages",
        type=int, 
        default=None,
        help="stop after consuming this many messages"
    )

    args = parser.parse_args()

    with open(args.schema, 'r') as f:
        raw_schema = json.load(f)
    schema = parse_schema(raw_schema)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=args.timeout_ms,
        key_deserializer=lambda k: k.decode() if k else None
    )

    print(f"Listening for events on topic '{args.topic}'...")
    count = 0
    for msg in consumer:
        count += 1
        try:
            record = schemaless_reader(BytesIO(msg.value), schema)
        except Exception:
            try:
                record = json.loads(msg.value.decode('utf-8'))
            except Exception:
                record = msg.value

        print(f"KEY={msg.key}  VALUE={record}")
        
        if args.max_messages and count >= args.max_messages:
            print(f"Consumed {count} messages, exiting.")
            break


if __name__ == "__main__":
    main()

