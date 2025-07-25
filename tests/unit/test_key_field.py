from click.testing import CliRunner
import pytest

from event_generator import main


def test_wrong_key_field_raises(mocker):
    """Unknown --key-field should raise KeyError inside script."""

    mocker.patch("event_generator.KafkaProducer")

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
            "nonexistent_field",
            "-b",
            "dummy:9092",
        ],
    )
    assert result.exit_code != 0
