# Real-Time Event Analytics Pipeline (Countly Infrastructure PoC)

## Overview
This project is a containerized, high-throughput data pipeline designed to ingest, buffer, and process real-time user events. Built as a proof-of-concept for scalable analytics infrastructure, it mimics the core data layer of enterprise-grade product analytics platforms.

The system is designed with **fault tolerance** and **high availability** in mind. It ensures zero data loss during sudden traffic spikes by utilizing Apache Kafka as a persistent message buffer, and optimizes database I/O by batch-inserting records into ClickHouse.

## Architecture

* **Event Generator (Python):** A multi-threaded traffic simulator that generates realistic user interactions (clicks, views, purchases) and continuously produces JSON payloads to a Kafka topic.
* **Message Broker (Apache Kafka in KRaft mode):** Acts as an intermediary buffer, decoupling the ingestion layer from the database layer. This prevents the database from being overwhelmed during peak loads.
* **Batch Ingestion Worker (Python Consumer):** A dedicated background process that polls the Kafka topic, aggregates events into memory, and performs efficient batch inserts via HTTP POST directly to ClickHouse.
* **Analytics Database (ClickHouse):** A column-oriented OLAP database configured with a `MergeTree` engine for lightning-fast analytical queries over massive datasets.
* **Observability Stack:** 
  * **Prometheus:** Scrapes real-time performance and system metrics directly from the ClickHouse endpoints.
  * **Grafana:** Provides data visualization and monitoring dashboards.
  * **Kafbat UI:** A web interface for real-time monitoring of Kafka clusters, brokers, and topics.

## Tech Stack
* **Infrastructure:** Docker, Docker Compose
* **Data Layer:** Apache Kafka (KRaft), ClickHouse
* **Application:** Python 3.11, `kafka-python`, `requests`
* **Observability:** Prometheus, Grafana, Kafbat UI

## Why this Architecture? (Design Decisions)
1. **Why Kafka?** Analytical databases perform poorly with frequent, single-row inserts. Kafka absorbs the initial write-heavy load (O(1) append operations) and allows the worker to consume messages at its own pace.
2. **Why Batching?** The Python worker aggregates messages into batches before executing a single `INSERT` query to ClickHouse. This drastically reduces the creation of small background files in ClickHouse's `MergeTree` engine, avoiding severe I/O performance degradation.
3. **Why KRaft Mode?** Removing the Zookeeper dependency simplifies the deployment topology, reduces infrastructure overhead, and speeds up cluster controller failovers.

## Prerequisites
* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)

## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/BekTim/mini-ver-countly.git
cd mini-ver-countly
```

### 2. Launch the Infrastructure
Start the entire pipeline, including the databases, message brokers, and the Python application:
```bash
docker compose up -d
```

### 3. Verify the Deployment
Ensure all services (Kafka, ClickHouse, Prometheus, Grafana, and the App) are running successfully:
```bash
./scripts/healthcheck.sh
docker compose ps
```