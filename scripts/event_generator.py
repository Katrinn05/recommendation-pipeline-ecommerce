import time
import json
import click
from io import BytesIO
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from fastavro import parse_schema, validate, schemaless_writer
from faker import Faker


@click.command()
@click.option(
    "--topic", "-t",
    default="product-clicks",
    help="Kafka topic to send events to"
)
@click.option(
    '--interval', '-i',
    default=1.0,
    type=float,
    help='Seconds to wait between events'
)
@click.option(
    "--count", "-c",
    default=1000,
    type=int,
    help="Total number of events to generate"
)
@click.option(
    "--schema", "-s",
    default="schemas/product_clicks.avsc",
    help="Path to the Avro schema file",
)
@click.option(
    '--key-field', '-k',
    default='user_id',
    help='Name of the field from the event to use as Kafka key'
)
@click.option(
    "--bootstrap-server", "-b",
    default="localhost:9092",
    help="Address of the Kafka broker (host:port)",
)
def main(topic, interval, count, schema, key_field, bootstrap_server):
    """
    Simple CLI tool to generate fake events and publish them to Kafka.
    """

    with open(schema, "r") as f:
        raw_schema = json.load(f)
    parsed_schema = parse_schema(raw_schema)

    fake = Faker()
    sent = 0

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                key_serializer=lambda k: str(k).encode('utf-8'),
                linger_ms=5,
                acks='all',
                retries=3
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

    for _ in range(count):
        event = {
            "user_id": fake.uuid4(),
            "product_id": fake.random_int(min=1, max=1000),
            "timestamp": int(time.time() * 1000),
        }

        if not validate(event, parsed_schema):
            raise ValueError(f"Event does not match schema: {event}")

        buffer = BytesIO()
        schemaless_writer(buffer, parsed_schema, event)
        avro_bytes = buffer.getvalue()

        key_value = event.get(key_field)

        if key_value is None:
            raise KeyError(f"Field '{key_field}' not found in event: {event}")
        
        future = producer.send(topic, key=key_value, value=avro_bytes)
        try:
            future.get(timeout=10)
        except KafkaError as e:
            click.echo(f"Failed to send event: {e}", err=True)
        
        sent += 1

        time.sleep(interval)

    producer.flush()
    producer.close()

    click.echo(f"Successfully sent {sent} events to topic '{topic}' "
           f"with interval={interval}s and key-field='{key_field}'")


if __name__ == "__main__":
    main()
