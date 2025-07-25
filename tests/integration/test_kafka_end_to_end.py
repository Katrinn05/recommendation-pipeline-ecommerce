"""
End-to-end test: requires running Kafka broker on localhost:9092
and pre-created topic 'cart-adds'.  
Mark with pytest -m integration to run selectively.
"""
from io import BytesIO

import pytest
from kafka import KafkaConsumer
from fastavro import schemaless_reader

pytestmark = pytest.mark.integration


def test_consumer_reads_valid_avro(cart_schema):
    consumer = KafkaConsumer(
        "cart-adds",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        key_deserializer=lambda k: k.decode() if k else None,
    )

    records = consumer.poll(timeout_ms=5000)
    if not records:
        pytest.fail("No messages in topic within 5s")
    
    first_batch = next(iter(records.values()))
    msg = first_batch[0]
    record = schemaless_reader(BytesIO(msg.value), cart_schema)

    assert set(record) == {"user_id", "product_id", "timestamp"}
    assert record["user_id"]
    assert isinstance(record["product_id"], int)
    assert isinstance(record["timestamp"], int)
