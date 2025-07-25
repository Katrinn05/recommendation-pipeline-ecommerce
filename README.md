# Stage 1 – Messaging Backbone & Event Simulation

This branch focuses on setting up the Kafka messaging backbone and generating synthetic events.

**Quickstart:**
```bash
docker compose up -d
make topics
python3 event_generator.py --topic product-clicks --count 100