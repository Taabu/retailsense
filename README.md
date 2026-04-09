# 🛒 RetailSense: Real-Time Retail Analytics Pipeline
A POC, end-to-end real-time data pipeline that simulates IoT sensor data from retail stores, processes it using Apache Spark, stores it in a cloud-native data lake (Iceberg on MinIO), and visualizes insights via Apache Superset.

Built to demonstrate modern data engineering patterns: Event-driven architecture, stream processing, schema evolution, and reproducible infrastructure.


# 🚀 Quick Start
Requires [Docker Deskptop](https://www.docker.com/products/docker-desktop/)

Get the entire pipeline running in 3 commands. No complex setup required. 


## 1. Clone the repository
```
git clone https://github.com/Taabu/retailsense.git && cd retailsense
```
## 2. Initialize environment variables
```
cp .env.example .env
```
## 3. Start the stack
```
docker compose up -d
```
Wait ~2 minutes for services to initialize.

Kafka UI: http://localhost:8082

MinIO Console: http://localhost:9002 (Login: minioadmin / minioadmin)

Superset Dashboard: http://localhost:8088 (Login: admin / admin). Set the dashboard auto-refresh interval to 1 minute

## 🏗️ Architecture Overview

```mermaid
flowchart TB
    %% Styling
    classDef producer fill:#4CAF50,stroke:#2E7D32,color:white,stroke-width:2px
    classDef bridge fill:#2196F3,stroke:#1565C0,color:white,stroke-width:2px
    classDef kafka fill:#FF9800,stroke:#EF6C00,color:white,stroke-width:2px
    classDef spark fill:#9C27B0,stroke:#6A1B9A,color:white,stroke-width:2px
    classDef storage fill:#607D8B,stroke:#455A64,color:white,stroke-width:2px
    classDef query fill:#00BCD4,stroke:#00838F,color:white,stroke-width:2px
    classDef viz fill:#E91E63,stroke:#AD1457,color:white,stroke-width:2px
    classDef support fill:#795548,stroke:#4E342E,color:white,stroke-width:2px

    %% Data Sources
    subgraph "Data Generation"
        Producer[📡 Sensor Producer<br/>Python + MQTT]:::producer
    end

    %% Ingestion Layer
    subgraph "Ingestion Layer"
        Mosquitto[Eclipse Mosquitto<br/>MQTT Broker]:::support
        Bridge[🌉 MQTT→Kafka Bridge<br/>Python]:::bridge
        Kafka[(Apache Kafka<br/>Event Buffer)]:::kafka
        Zookeeper[(Zookeeper<br/>Coordination)]:::support
    end

    %% Processing Layer
    subgraph "Stream Processing"
        Spark[⚡ Spark Processor<br/>Structured Streaming]:::spark
    end

    %% Storage Layer
    subgraph "Data Lake"
        MinIO[(MinIO<br/>S3-Compatible Storage)]:::storage
        Iceberg[📦 Apache Iceberg<br/>ACID Tables]:::storage
        HiveDB[(PostgreSQL<br/>Metastore DB)]:::support
        HiveMS[Hive Metastore<br/>Service]:::support
    end

    %% Serving Layer
    subgraph "Query & Visualization"
        Trino[🔍 Trino<br/>SQL Query Engine]:::query
        Superset[📊 Apache Superset<br/>BI Dashboard]:::viz
        SupersetDB[(PostgreSQL<br/>Superset DB)]:::support
    end

    %% Data Flow
    Producer -->|MQTT Events| Mosquitto
    Mosquitto -->|Subscribe| Bridge
    Bridge -->|Publish| Kafka
    Zookeeper -.->|Manage| Kafka
    Kafka -->|Consume| Spark
    Spark -->|Write S3A| MinIO
    MinIO -->|Store| Iceberg
    HiveMS -->|Manage Metadata| HiveDB
    Spark -->|Register Tables| HiveMS
    Trino -->|Query| Iceberg
    Trino -->|Read Metadata| HiveMS
    Superset -->|Visualize| Trino
    Superset -->|Store Config| SupersetDB

    %% Legend
    subgraph "Legend"
        L1[📡 Data Source]
        L2[🌉 Ingestion]
        L3[⚡ Processing]
        L4[📦 Storage]
        L5[🔍 Query]
        L6[📊 Visualization]
    end
```
The system follows a Lambda-like architecture optimized for real-time analytics:

- Data Generation (Producer): Simulates 100+ sensor events/second (foot traffic, dwell time, demographics) from 3 stores with 4 zones each.

- Ingestion (Bridge): Subscribes to MQTT topics and forwards messages to Apache Kafka.

- Processing (Spark Processor):
Consumes from Kafka.
Cleans noisy data (filters invalid dwell times).
Enriches with metadata (peak hours, weekends).
Aggregates into 5-minute windows.
Writes to Apache Iceberg tables on MinIO (S3-compatible).

- Serving (Trino + Superset):
Trino acts as the SQL query engine over the Iceberg lakehouse.

- Superset provides real-time dashboards and visualization.

### Tech Stack
| Layer	| Technology | Purpose |
| -------- | ------- | ------- |
| Message Bus | Apache Kafka + Zookeeper | High-throughput event buffering |
| IoT Protocol | Eclipse Mosquitto (MQTT) | Lightweight sensor communication |
| Stream Processing | Apache Spark 3.5 | Structured Streaming & Transformations |
| Data Lake | Apache Iceberg | ACID transactions, schema evolution |
| Storage | MinIO | S3-compatible object storage |
| Query Engine | Trino | Distributed SQL analytics |
| Visualization | Apache Superset | BI & Dashboards |
| Infrastructure | Docker Compose | Reproducible local environment |

## 📊 Live Dashboards
Upon startup, the pipeline automatically populates the following metrics in Superset:

🔴 Live Visitor Count: Total visitors across all stores in the current 5-min window.

🏢 Store Performance: Bar chart comparing visitor volume per store.

📍 Zone Heatmap: Pie chart showing traffic distribution across store zones.


🛠️ Project Structure
```
retailsense/
├── modules/
│   ├── producer/       # Sensor simulation (Python)
│   ├── bridge/         # MQTT to Kafka bridge (Python)
│   └── processor/      # Spark streaming logic (Python)
├── infra/
│   └── docker/         # Docker configs, init scripts, and secrets
│       ├── postgres/
│       ├── hive/
│       ├── trino/
│       └── superset/
│           └── backups/        # Superset dashboard backups (for reproducibility)
├── docker-compose.yml  # Orchestrator
├── .env.example        # Environment template
└── README.md
```
## 🧪 Reproducibility & Testing
This project is designed to be 100% reproducible on any machine with Docker installed.


Automated Tests: Each module includes a pytest suite.
```
cd modules/producer && uv run pytest
```
```
cd modules/bridge && uv run pytest
```
```
cd modules/processor && uv run pytest
```
Dashboard Restoration: The Superset dashboard is exported as a backup (infra/docker/superset/backups/) and automatically restored on every container startup.
## 📝 Configuration
Key environment variables are defined in .env.example

Defaults are safe for local development

## 📄 License
MIT License. See LICENSE for details.

## 🤖 Created with the Help of [Lumo](https://lumo.proton.me/)
