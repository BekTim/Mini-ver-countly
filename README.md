# Real-Time Event Analytics Pipeline

## Overview
This project is a containerized, high-throughput data pipeline designed to ingest, buffer, and process real-time user events. Built as a proof-of-concept for scalable analytics infrastructure, it mimics the core data layer of enterprise-grade product analytics platforms.

The system is designed with **fault tolerance** and **high availability** in mind. It ensures zero data loss during sudden traffic spikes by utilizing Apache Kafka as a persistent message buffer, and optimizes database I/O by batch-inserting records into ClickHouse.

## Architecture

* **API Gateway (FastAPI):** A lightweight asynchronous REST API (`POST /track`) that catches incoming JSON events and immediately produces them to a Kafka topic.
* **Message Broker (Apache Kafka in KRaft mode):** Acts as an intermediary buffer, decoupling the ingestion layer from the database layer. This prevents the database from being overwhelmed during peak loads.
* **Batch Ingestion Worker (Python):** A dedicated consumer process that polls the Kafka topic, aggregates events into memory, and performs efficient batch inserts via HTTP POST.
* **Analytics Database (ClickHouse):** A column-oriented OLAP database configured with a `MergeTree` engine for lightning-fast analytical queries over massive datasets.
* **Management UI (Kafbat UI):** A web interface for real-time monitoring of Kafka clusters, brokers, and topics.

## Tech Stack
* **Infrastructure:** Docker, Docker Compose
* **Data Layer:** Apache Kafka (KRaft), ClickHouse
* **Application:** Python 3.11, FastAPI, `kafka-python`
* **Observability:** Kafbat UI (Prometheus & Grafana integration planned)

## Why this Architecture? (Design Decisions)
1.  **Why Kafka?** Analytical databases perform poorly with frequent, single-row inserts. Kafka absorbs the initial write-heavy load (O(1) append operations) and allows the worker to consume messages at its own pace.
2.  **Why Batching?** The Python worker aggregates messages into batches (e.g., 10-50 events) before executing a single `INSERT` query to ClickHouse. This drastically reduces the creation of small background files in ClickHouse's `MergeTree` engine, avoiding severe performance degradation.
3.  **Why KRaft Mode?** Removing the Zookeeper dependency simplifies the deployment topology, reduces infrastructure overhead, and speeds up cluster controller failovers.

## Prerequisites
* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)

## Getting Started

### 1. Clone the repository
```bash
git clone [https://github.com/your-username/countly-event-pipeline.git](https://github.com/your-username/countly-event-pipeline.git)
cd countly-event-pipeline
