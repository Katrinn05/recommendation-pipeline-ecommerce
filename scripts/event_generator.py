#!/usr/bin/env python3
import time
import json
import click
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from fastavro import parse_schema, validate
from faker import Faker


@click.command()
@click.option(
    "--topic", "-t", default="product-clicks", help="Kafka topic to send events to"
)
@click.option("--rate", "-r", default=50, type=int, help="Events per second to produce")
@click.option(
    "--count", "-c", default=1000, type=int, help="Total number of events to generate"
)
@click.option(
    "--schema",
    "-s",
    default="schemas/product_clicks.avsc",
    help="Path to the Avro schema file",
)
@click.option(
    "--bootstrap-server",
    "-b",
    default="localhost:9092",
    help="Address of the Kafka broker (host:port)",
)
def main(topic, rate, count, schema, bootstrap_server):
    """
    Simple CLI tool to generate fake events and publish them to Kafka.
    """

    # Load and parse the Avro schema
    with open(schema, "r") as f:
        raw_schema = json.load(f)
    parsed_schema = parse_schema(raw_schema)

    fake = Faker()
    sent = 0

    # Create a Kafka producer with retry logic
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=5,
                acks="all",
                retries=3,
            )
            break
        except NoBrokersAvailable:
            click.echo(f"[{attempt}/{max_retries}] Kafka broker unavailable, retrying…")
            time.sleep(1)
    else:
        click.echo(
            f"Kafka broker is still unavailable after {max_retries} attempts", err=True
        )
        raise SystemExit(1)

    # Generate and send events
    for _ in range(count):
        # Create a single event matching the Avro schema
        event = {
            "user_id": fake.uuid4(),
            "product_id": fake.random_int(min=1, max=1000),
            "timestamp": int(time.time() * 1000),
        }

        # Validate event against the parsed Avro schema
        if not validate(event, parsed_schema):
            raise ValueError(f"Event does not match schema: {event}")

        # Send the event to Kafka (non-blocking)
        producer.send(topic, value=event)
        sent += 1

        # Throttle to achieve the desired rate
        time.sleep(1.0 / rate)

    # Ensure all buffered messages are sent before exiting
    producer.flush()
    producer.close()

    click.echo(f"Successfully sent {sent} events to topic '{topic}'")


if __name__ == "__main__":
    main()
