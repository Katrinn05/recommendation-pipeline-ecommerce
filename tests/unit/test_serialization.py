from io import BytesIO

from click.testing import CliRunner
from fastavro import schemaless_reader
import pytest

from event_generator import main


def test_send_receives_avro_bytes(cart_schema, mocker):
    """Producer.send must receive binary Avro encoded value and correct key."""
    fake_producer = mocker.Mock()
    mocker.patch("kafka.KafkaProducer", return_value=fake_producer)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "-t",
            "cart-adds",
            "-s",
            "schemas/cart_adds.avsc",
            "-i",
            "0",
            "-c",
            "1",
            "-k",
            "user_id",
            "-b",
            "dummy:9092",
        ],
    )
    assert result.exit_code == 0

    _, kwargs = fake_producer.send.call_args
    key = kwargs["key"]
    value_bytes = kwargs["value"]

    assert isinstance(key, str) and len(key) > 0

    assert isinstance(value_bytes, (bytes, bytearray))

    record = schemaless_reader(BytesIO(value_bytes), cart_schema)
    assert set(record) == {"user_id", "product_id", "timestamp"}

