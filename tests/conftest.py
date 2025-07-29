import json
import os
from pathlib import Path
import pytest
from fastavro import parse_schema
import sys, types
from unittest.mock import Mock

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SCHEMA_DIR = ROOT_DIR / "schemas"


@pytest.fixture(scope="session")
def cart_schema():
    """Parsed Avro schema for cart_adds events."""
    with open(SCHEMA_DIR / "cart_adds.avsc") as f:
        return parse_schema(json.load(f))


@pytest.fixture(scope="session")
def product_clicks_schema():
    with open(SCHEMA_DIR / "product_clicks.avsc") as f:
        return parse_schema(json.load(f))


@pytest.fixture(scope="session")
def purchases_schema():
    with open(SCHEMA_DIR / "purchases.avsc") as f:
        return parse_schema(json.load(f))
    
@pytest.fixture(autouse=True)
def _patch_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_: None)


