import json
import time
import random
from typing import List

import click
from click.core import ParameterSource
from io import BytesIO

import kafka
from kafka.errors import NoBrokersAvailable
from fastavro import parse_schema, validate, schemaless_writer
from faker import Faker
import numpy as np


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
    help=(
        "Average delay between events in seconds.  "
        "Actual delays are sampled from an exponential distribution "
        "with this mean to simulate a more realistic, bursty stream."
    ),
)
@click.option(
    "--count", "-c",
    default=100,
    type=int,
    help="Total number of events to generate",
)
@click.option(
    "--num-users",
    default=500,
    type=int,
    show_default=True,
    help="Number of distinct users to simulate.  Users will be sampled with an activity distribution.",
)
@click.option(
    "--num-products",
    default=1000,
    type=int,
    show_default=True,
    help="Number of distinct product IDs to sample from.",
)
@click.option(
    "--user-shape",
    default=2.0,
    type=float,
    show_default=True,
    help="Shape parameter for the Pareto distribution controlling user activity (higher => more skew).",
)
@click.option(
    "--product-shape",
    default=2.5,
    type=float,
    show_default=True,
    help="Shape parameter for the Pareto distribution controlling product popularity (higher => more skew).",
)
@click.option(
    "--seed",
    default=None,
    type=int,
    help="Random seed for reproducibility.  If unset, system randomness is used.",
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
def main(
    ctx,
    topic: str,
    schema: str,
    interval: float,
    count: int,
    num_users: int,
    num_products: int,
    user_shape: float,
    product_shape: float,
    seed: int | None,
    key_field: str,
    bootstrap_server: str,
):
    """
    Generate synthetic events with more realistic distributions and publish them to Kafka.

    Users and products are sampled from fixed pools according to Pareto distributions
    (`user_shape` and `product_shape`).  Inter‑event delays follow an exponential
    distribution with mean `interval` seconds.  A fixed random seed can be
    specified for reproducibility.
    """
    # Abort if any required option was not provided explicitly
    for param in ('topic', 'schema'):
        if ctx.get_parameter_source(param) != ParameterSource.COMMANDLINE:
            raise click.Abort()

    # Load and parse Avro schema
    with open(schema, "r") as f:
        raw_schema = json.load(f)
    parsed_schema = parse_schema(raw_schema)

    # Set random seed if provided
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    fake = Faker()
    sent = 0

    # Precompute pools and weights for users and products
    user_ids: List[str] = [str(fake.uuid4()) for _ in range(num_users)]
    user_activity = np.random.pareto(a=user_shape, size=num_users)
    user_activity = user_activity / user_activity.sum()

    product_ids = list(range(1, num_products + 1))
    product_popularity = np.random.pareto(a=product_shape, size=num_products)
    product_popularity = product_popularity / product_popularity.sum()

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
        # Sample a user and product based on the pre‑computed distributions
        user_id = random.choices(user_ids, weights=user_activity, k=1)[0]
        product_id = random.choices(product_ids, weights=product_popularity, k=1)[0]
        event = {
            "user_id": user_id,
            "product_id": product_id,
            "timestamp": int(time.time() * 1000),
        }

        # Validate against the Avro schema
        if not validate(event, parsed_schema):
            raise ValueError(f"Event does not match schema: {event}")

        # Serialize event to Avro
        buffer = BytesIO()
        schemaless_writer(buffer, parsed_schema, event)
        avro_bytes = buffer.getvalue()

        # Determine the key value
        key_value = event.get(key_field)
        if key_value is None:
            raise KeyError(f"Field '{key_field}' not found in event: {event}")

        # Send to Kafka
        future = producer.send(topic, key=key_value, value=avro_bytes)
        try:
            future.get(timeout=10)
        except Exception as e:
            click.echo(f"Failed to send event: {e}", err=True)

        sent += 1
        # Sleep for a random delay sampled from an exponential distribution
        delay = np.random.exponential(scale=interval)
        time.sleep(delay)

    producer.flush()
    producer.close()

    click.echo(
        f"Successfully sent {sent} events to topic '{topic}' "
        f"with average interval={interval}s and key-field='{key_field}'"
    )


if __name__ == "__main__":
    main()

