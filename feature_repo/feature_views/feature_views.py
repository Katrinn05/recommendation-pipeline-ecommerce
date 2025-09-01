"""
Definition of feature entities and feature views for the Stage 2 proof of concept.

This module uses Feast to define a feature store.  The configuration here assumes
that aggregated feature values will be stored in partitioned Parquet files under
./data/offline and loaded into a Redis online store for low-latency access.
"""

from datetime import timedelta

from feast import Entity, Field, FeatureView, FileSource, ValueType
from feast.types import Int64

# Define an entity that uniquely identifies a user in the recommendation system.
user = Entity(
    name="user_id",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
    description="Unique identifier for each user"
)

# Define the offline source for the aggregated feature dataset.  The Parquet
# files will be partitioned by date (e.g., partition_date=2025-07-31).
user_daily_source = FileSource(
    path="../data/offline",
    timestamp_field="event_timestamp",
    created_timestamp_column="ingestion_time",
)

# Create a FeatureView that groups together daily aggregated metrics for recommendation.
user_daily_features = FeatureView(
    name="user_daily_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(
            name="click_count_24h",
            dtype=Int64,
            description="Number of product click events in the last 24 hours",
        ),
        Field(
            name="cart_add_count_24h",
            dtype=Int64,
            description="Number of add-to-cart events in the last 24 hours",
        ),
        Field(
            name="purchase_count_24h",
            dtype=Int64,
            description="Number of purchase events in the last 24 hours",
        ),
        Field(
            name="distinct_products_24h", 
            dtype=Int64, 
            description="Unique products touched in last day"),
    ],
    online=True,
    source=user_daily_source,
)
