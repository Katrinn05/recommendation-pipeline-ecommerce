# Purpose: end-to-end test for Stage 2
# Flow: Kafka <- event_generator.py -> feature_engineering.py -> Parquet partitions
#
# Test logic:
#   1) (Optional) start services via `docker compose up -d` when START_SERVICES=1
#   2) Wait for Kafka @ localhost:9092 to be reachable
#   3) Create topics (idempotent) via Kafka Admin API
#   4) Start the streaming pipeline (feature_engineering.py) in background
#   5) Produce a small batch of events to each topic (event_generator.py)
#   6) Gracefully stop the pipeline with SIGINT so `flush()` writes the parquet
#   7) Assert that a partitioned parquet file exists for "today" and has expected columns

import os
import sys
import time
import signal
import glob
import subprocess
from pathlib import Path

import pytest
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

pytestmark = pytest.mark.integration  # ensure test is collected with -m integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = REPO_ROOT / "schemas"
PIPELINE = REPO_ROOT / "pipelines" / "feature_engineering.py"
EVENT_GEN = REPO_ROOT / "scripts" / "event_generator.py"
OFFLINE_ROOT = REPO_ROOT / "data" / "offline"

TOPICS = ["product-clicks", "cart-adds", "purchases"]  # must match pipeline mapping


def _compose_up_if_requested():
    """Optionally start dockerized services to make the test self-sufficient."""
    if os.environ.get("START_SERVICES") == "1":
        # Best-effort; if compose is missing, we'll fail later on wait_for_kafka()
        subprocess.run(["docker", "compose", "up", "-d"], cwd=str(REPO_ROOT), check=False)


def _wait_for_kafka(timeout=60):
    """Wait until Kafka is reachable by creating a short-lived producer."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            prod = KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=0, request_timeout_ms=2000)
            prod.close()
            return True
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    if last_err:
        print(f"Kafka not ready: {last_err}")
    return False


def _ensure_topics(topics, partitions=3, replication_factor=1):
    """Create topics if they don't exist, ignore 'already exists' errors."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=5000)
    try:
        existing = set(t.topic for t in admin.list_topics())
    except Exception:
        existing = set()
    new = [
        NewTopic(name=t, num_partitions=partitions, replication_factor=replication_factor)
        for t in topics
        if t not in existing
    ]
    if new:
        try:
            admin.create_topics(new_topics=new, validate_only=False)
        except Exception:
            # Kafka may race-create them in parallel; ignore benign errors
            pass
    admin.close()


def _start_pipeline():
    """Start the streaming pipeline process in background."""
    env = os.environ.copy()
    # Ensure the pipeline writes into repo-root/data/offline
    proc = subprocess.Popen(
        [sys.executable, str(PIPELINE)],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc


def _stop_pipeline(proc: subprocess.Popen, timeout=20):
    """Try to stop with SIGINT so the pipeline executes its graceful flush."""
    try:
        if hasattr(signal, "SIGINT"):
            proc.send_signal(signal.SIGINT)
        else:
            proc.terminate()
    except Exception:
        pass
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


def _produce_events(topic: str, count: int, interval: float, schema_file: str):
    """Call event_generator.py as a subprocess to produce Avro-encoded messages."""
    cmd = [
        sys.executable, str(EVENT_GEN),
        "-t", topic,
        "-s", str(SCHEMAS_DIR / schema_file),
        "-c", str(count),
        "-i", str(interval),
        "-b", BOOTSTRAP,
    ]
    # event_generator requires both --topic and --schema to be explicitly passed
    subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)


def _latest_parquet_path():
    """Find the newest partitioned parquet written by the pipeline."""
    pattern = str(OFFLINE_ROOT / "partition_date=*/user_daily_features.parquet")
    matches = glob.glob(pattern)
    if not matches:
        return None
    # newest by mtime
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return Path(matches[0])


@pytest.fixture(autouse=True)
def _skip_unless_enabled():
    """Opt-in switch for integration runs."""
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Integration disabled (set RUN_INTEGRATION=1 to enable)")


def test_kafka_to_parquet_end_to_end(tmp_path):
    # 1) Optionally start services (Kafka, Redis, etc.)
    _compose_up_if_requested()

    # 2) Wait for Kafka
    assert _wait_for_kafka(timeout=60), "Kafka did not become ready on time"

    # 3) Ensure topics exist
    _ensure_topics(TOPICS)

    # 4) Start pipeline
    proc = _start_pipeline()
    time.sleep(2.0)  # give the consumer time to subscribe

    try:
        # 5) Produce a small batch per topic
        _produce_events("product-clicks", count=60, interval=0.005, schema_file="product_clicks.avsc")
        _produce_events("cart-adds",     count=30, interval=0.005, schema_file="cart_adds.avsc")
        _produce_events("purchases",     count=10, interval=0.005, schema_file="purchases.avsc")

        # 6) Stop pipeline to trigger final flush()
        _stop_pipeline(proc)

        # 7) Wait a moment for filesystem
        time.sleep(1.0)

        # 8) Verify parquet output exists and schema matches expectation
        p = _latest_parquet_path()
        assert p is not None, "No partitioned parquet found under data/offline/*"

        df = pd.read_parquet(p)
        expected_cols = {
            "user_id", "event_date",
            "click_count_24h", "cart_add_count_24h", "purchase_count_24h",
            "event_timestamp", "ingestion_time",
        }
        assert expected_cols.issubset(set(df.columns)), f"Missing columns in {p.name}"

        # Some rows must be present (we produced 100 messages across topics,
        # aggregated per user/date, which should result in >= 1 row)
        assert len(df) >= 1, "Empty parquet after end-to-end run"

    finally:
        # Ensure the child process is gone in case of assertion failures
        if proc.poll() is None:
            _stop_pipeline(proc, timeout=5)

