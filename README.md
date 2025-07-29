# Stage 1 – Messaging Backbone & Event Simulation

This branch focuses on setting up the Kafka messaging backbone and generating synthetic events.

**Quickstart:**
```bash
docker compose up -d
make topics
python3 scripts/event_generator.py \
  --topic cart-adds \
  --schema schemas/cart_adds.avsc \
  --interval 0.5 \
  --count 500 \
  --key-field user_id \
  --bootstrap-server localhost:9092

# (Optional) Run the smoke-test consumer
make smoke

# Run unit tests only
make unit-test
# Run integration tests
make integration-test
# Run all tests
make test