import time
from click.testing import CliRunner
import pytest
from unittest.mock import call

from event_generator import main


def test_interval_sleep_called_exact_number_of_times(mocker):
    """time.sleep must be called count times with exact interval."""
    mocker.patch("event_generator.KafkaProducer")          # no real Kafka
    mocked_sleep = mocker.patch("event_generator.time.sleep")

    runner = CliRunner()
    runner.invoke(
        main,
        [
            "-t",
            "cart-adds",
            "-s",
            "schemas/cart_adds.avsc",
            "-i",
            "0.05",
            "-c",
            "3",
            "-b",
            "dummy:9092",
        ],
    )
    expected_calls = [call(0.05), call(0.05), call(0.05)]
    assert mocked_sleep.call_args_list == expected_calls

