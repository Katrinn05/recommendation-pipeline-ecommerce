from pipelines.feature_engineering import flush
import os, shutil, pandas as pd

def test_flush_creates_partition_and_columns(tmp_path):
    aggregates = {
        ("u-1","2025-07-31"): {"clicks": 3, "carts": 1, "purchases": 0},
        ("u-2","2025-07-31"): {"clicks": 0, "carts": 2, "purchases": 1},
    }
    outdir = tmp_path / "offline"
    flush(aggregates, output_root=str(outdir))
    # verify partition directory
    part = outdir / "partition_date=2025-07-31" / "user_daily_features.parquet"
    assert part.exists()
    df = pd.read_parquet(part)
    assert set(df.columns) >= {
        "user_id","event_date","click_count_24h",
        "cart_add_count_24h","purchase_count_24h",
        "event_timestamp","ingestion_time"
    }
