# Recommendation Pipeline for E-commerce

A production-grade, event-driven platform for ingesting, processing, and serving recommendations at scale.

## Project Goals
1. **High-throughput ingestion** of click-stream and catalog updates via Apache Kafka.  
2. **Stream and batch feature computation** for real-time and offline models.  
3. **Modular model registry and serving** so any team can plug in a recommender.  
4. **Observability by design**—metrics, traces, and structured logs out of the box.

## Overview

This repository provides the infrastructure and tools to build a scalable recommendation pipeline. Key stages include message ingestion via Kafka, feature engineering, model training, real-time serving, and observability.

## Prerequisites

* Docker & Docker Compose (v2.0+)
* Python 3.8+
* pip (or pipenv/virtualenv)

## Getting Started

1. **Start Kafka broker**

   ```bash
   docker compose up -d
   ```
2. **Bootstrap Kafka topics**

   ```bash
   make create-topics
   ```
3. **Generate synthetic events**

   ```bash
   python3 event_generator.py --topic product-clicks --count 500 --rate 50
   ```

## Roadmap

| Stage | Branch    | Description                                          |
| ----- | --------- | ---------------------------------------------------- |
| 0     | `main`    | Repository setup and foundational documentation      |
| 1     | `Stage-1` | Messaging backbone & event simulation                |
| 2     | `Stage-2` | Feature engineering & feature store proof of concept |
| 3     | `Stage-3` | Model training pipelines & registry integration      |
| 4     | `Stage-4` | Real-time serving & API development                  |
| 5     | `Stage-5` | Observability, CI/CD, and monitoring                 |

> **Note:** Detailed documentation for each stage is available in its respective branch (e.g., the `Stage-1` branch contains specific instructions and examples).
