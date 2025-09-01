# ===== Variables =====
PY ?= python3
BOOTSTRAP ?= localhost:9092
COUNT ?= 1000
INTERVAL ?= 0.5
NUM_USERS ?= 500
NUM_PRODUCTS ?= 1000
USER_SHAPE ?= 2.0
PRODUCT_SHAPE ?= 2.5
SEED ?= 42

# ===== Stage 1 — Event Generation =====
.PHONY: gen-clicks gen-cart gen-purchase gen-events

gen-clicks: ## generate product-clicks with realistic distributions
	$(PY) scripts/event_generator.py \
	  --topic product-clicks \
	  --schema schemas/product_clicks.avsc \
	  --count $(COUNT) \
	  --interval $(INTERVAL) \
	  --num-users $(NUM_USERS) \
	  --num-products $(NUM_PRODUCTS) \
	  --user-shape $(USER_SHAPE) \
	  --product-shape $(PRODUCT_SHAPE) \
	  --seed $(SEED) \
	  --bootstrap-server $(BOOTSTRAP)

gen-cart:
	$(PY) scripts/event_generator.py \
	  --topic cart-adds \
	  --schema schemas/cart_adds.avsc \
	  --count $(COUNT) \
	  --interval $(INTERVAL) \
	  --num-users $(NUM_USERS) \
	  --num-products $(NUM_PRODUCTS) \
	  --user-shape $(USER_SHAPE) \
	  --product-shape $(PRODUCT_SHAPE) \
	  --seed $(SEED) \
	  --bootstrap-server $(BOOTSTRAP)

gen-purchase:
	$(PY) scripts/event_generator.py \
	  --topic purchases \
	  --schema schemas/purchases.avsc \
	  --count $(COUNT) \
	  --interval $(INTERVAL) \
	  --num-users $(NUM_USERS) \
	  --num-products $(NUM_PRODUCTS) \
	  --user-shape $(USER_SHAPE) \
	  --product-shape $(PRODUCT_SHAPE) \
	  --seed $(SEED) \
	  --bootstrap-server $(BOOTSTRAP)

gen-events: gen-clicks gen-cart gen-purchase ## generate all topics

# ===== Stage 2 — Feature Streaming =====
.PHONY: features-stream

features-stream: ## run streaming feature engineering
	$(PY) pipelines/feature_engineering.py \
	  --bootstrap-server $(BOOTSTRAP) \
	  --output-dir data/offline \
	  --flush-every 10000 \
	  --flush-seconds 60 \
	  --log-level INFO

# ===== Tests =====
.PHONY: unit-test integration-test test

unit-test: ## run unit tests
	pytest -q tests/unit

integration-test: ## run integration tests
	pytest -q tests/integration -m kafka --bootstrap $(BOOTSTRAP)

test: unit-test integration-test ## run all tests

# ===== Utilities =====
.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'
