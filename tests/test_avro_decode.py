import json
from io import BytesIO
from fastavro import parse_schema, schemaless_writer, schemaless_reader

def roundtrip(schema_path: str, event: dict):
    with open(schema_path, "r") as f:
        schema = parse_schema(json.load(f))
    buf = BytesIO()
    schemaless_writer(buf, schema, event)
    buf.seek(0)
    out = schemaless_reader(buf, schema)
    return out

def test_product_click_decode():
    event = {"user_id": "u-1", "product_id": 42, "timestamp": 1720000000000}
    decoded = roundtrip("schemas/product_clicks.avsc", event)
    assert decoded["user_id"] == "u-1"
    assert decoded["product_id"] == 42
