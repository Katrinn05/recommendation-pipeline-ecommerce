from pathlib import Path

def test_offline_layout_exists():
    root = Path("data/offline")
    root.mkdir(parents=True, exist_ok=True)
    assert root.exists()
