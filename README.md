# Recommendation Pipeline for E-commerce

A production-grade, event-driven platform for ingesting, processing, and serving recommendations at scale.

---

## 🎯 Project Goals
1. **High-throughput ingestion** of click-stream and catalog updates via Apache Kafka.  
2. **Stream and batch feature computation** for real-time and offline models.  
3. **Modular model registry and serving** so any team can plug in a recommender.  
4. **Observability by design**—metrics, traces, and structured logs out of the box.

---

## 🗺️ Roadmap

| Stage | Status | High-level Deliverables |
|-------|--------|-------------------------|
| **0 – Bootstrap** | ✅ *(this commit)* | • Repository skeleton<br>• Documentation foundations |
| **1 – Messaging Backbone & Event Simulation** | ⬜ | • Local single-node Kafka via Docker Compose<br>• Event generator script producing realistic product-interaction events |
| **2 – Feature Engineering** | ⬜ | • Stream processors (Kafka Streams / Flink)<br>• Feature store PoC |
| **3 – Model Training & Registry** | ⬜ | • Offline pipelines (Spark/ Pandas)<br>• MLflow registry integration |
| **4 – Real-time Serving** | ⬜ | • gRPC/REST recommendation service<br>• A/B experiment hooks |
| **5 – Observability & CI/CD** | ⬜ | • Grafana dashboards<br>• GitHub Actions for build/test/deploy |

> **Tip:** Each stage lives in its own branch until it is production-ready, then merges to **main** via pull request.

---

## 📦 Stage 1 in Detail

### Objective
Stand up a reliable messaging backbone (**Apache Kafka**) and produce a continuous stream of synthetic events so downstream components have data from day one.

### Task-by-task Breakdown
| # | Task | Success criteria |
|---|------|------------------|
| 1 | **Spin up Kafka & Zookeeper** using Docker Compose | `docker compose ps` shows healthy `kafka` and `zookeeper` containers |
| 2 | **Create bootstrap topics** (`product-clicks`, `cart-adds`, `purchases`, etc.) | `kafka-topics --list` prints all required topics |
| 3 | **Write an event-generator CLI** (`scripts/event_generator.py`) | • Configurable throughput (events/sec)<br>• JSON payloads validated against Avro/JSON Schema<br>• Sends to correct topic |
| 4 | **Add Make targets & README usage section** | `make up` brings the stack up; `make simulate` starts the producer |
| 5 | **Smoke-test consumer** (`scripts/print_events.py`) | Events printed to stdout with correct schema |
| 6 | **CI gate: Stage-1 test job** | GitHub Actions workflow passes on every PR |

### Local Quick-start (once Stage 1 is done)

```bash
# Bring everything up
docker compose up -d

# Generate 500 events at ~50 events/s
python scripts/event_generator.py --count 500 --rate 50

# Peek at the stream
python scripts/print_events.py --topic product-clicks

