import json
import time
import click
from click.core import ParameterSource
from io import BytesIO
import kafka
from kafka.errors import NoBrokersAvailable
from fastavro import parse_schema, validate, schemaless_writer
from faker import Faker


@click.command()
@click.option(
    "--topic", "-t",
    default=None,
    help="Kafka topic to send events to",
)
@click.option(
    "--schema", "-s",
    default=None,
    help="Path to the Avro schema file",
)
@click.option(
    "--interval", "-i",
    default=1.0,
    type=float,
    help='Seconds to wait between events',
)
@click.option(
    "--count", "-c",
    default=100,
    type=int,
    help="Total number of events to generate",
)
@click.option(
    "--key-field", "-k",
    default='user_id',
    show_default=True,
    help='Name of the field from the event to use as Kafka key',
)
@click.option(
    "--bootstrap-server", "-b",
    default="localhost:9092",
    show_default=True,
    help="Address of the Kafka broker (host:port)",
)
@click.pass_context
def main(ctx, topic, schema, interval, count, key_field, bootstrap_server):
    """
    Simple CLI tool to generate fake events and publish them to Kafka.
    """
    # Abort if any required option was not provided explicitly
    for param in ('topic', 'schema'):
        if ctx.get_parameter_source(param) != ParameterSource.COMMANDLINE:
            raise click.Abort()

    # Load and parse Avro schema
    with open(schema, "r") as f:
        raw_schema = json.load(f)
    parsed_schema = parse_schema(raw_schema)

    fake = Faker()
    sent = 0

    # Initialize Kafka producer with retries
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            producer = kafka.KafkaProducer(
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

    # Generate and send events
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
        except Exception as e:
            click.echo(f"Failed to send event: {e}", err=True)

        sent += 1
        time.sleep(interval)

    producer.flush()
    producer.close()

    click.echo(
        f"Successfully sent {sent} events to topic '{topic}' "
        f"with interval={interval}s and key-field='{key_field}'"
    )


if __name__ == "__main__":
    main()


