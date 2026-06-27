# 🎵 Real-Time Spotify Analysis Pipeline

A production-grade, end-to-end data engineering pipeline that streams, stores, transforms, and visualizes Spotify event data in real time — fully containerized with Docker and orchestrated with Apache Airflow.

---

## 📐 Architecture Overview

```
simulator/producer.py
       │
       ▼
   Apache Kafka  ◄──── Kafdrop (UI Monitoring)
       │
       ▼
consumer/kafka-to-minio.py
       │
       ▼
   MinIO (Object Store)
   spotify/bronze/date=YYYY-MM-DD/hour=HH/*.json
       │
       ▼
 Airflow DAG: spotify_minio_to_snowflake_bronze
   ├── extract_data       (PythonOperator)
   └── load_raw_to_snowflake (PythonOperator)
       │
       ▼
 Snowflake: SPOTIFY_DB / BRONZE / SPOTIFY_EVENTS_BRONZE
       │
       ▼
   dbt (silver → gold models)
       │
       ▼
   Power BI (Visualization)
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| **Containerization** | Docker | All services run as isolated containers |
| **Data Simulation** | Python (`producer.py`) | Generates mock Spotify events and streams to Kafka |
| **Data Streaming** | Apache Kafka | Real-time event streaming (broker: `kafka:9092`) |
| **Kafka Monitoring** | Kafdrop | Web UI to inspect Kafka topics, partitions, and messages |
| **Data Storage** | MinIO (S3-compatible) | Local object store — partitioned by `date` and `hour` |
| **Orchestration** | Apache Airflow | Schedules and monitors the MinIO → Snowflake pipeline |
| **Data Warehouse** | Snowflake | Cloud warehouse hosting Bronze, Silver, and Gold layers |
| **Data Transformation** | dbt | SQL models to transform data across medallion layers |
| **Visualization** | Microsoft Power BI | Dashboards on top of Gold-layer Snowflake tables |

---

## 🔄 Pipeline Walkthrough

### 1. 🎲 Data Simulation — `simulator/producer.py`
- Generates realistic Spotify user events: `play`, `pause`, `skip`, `add_to_playlist`, etc.
- Each event contains: `SONG_NAME`, `EVENT_TYPE`, `DEVICE_TYPE`, `COUNTRY`, `TIMESTAMP`.
- Events are published to a **Kafka topic** every few seconds, simulating a live data stream.

### 2. 📡 Streaming — Apache Kafka
- Kafka (broker: `kafka:9092`) receives events from the producer and makes them available to consumers.
- **Kafdrop** (port `9000`) provides a browser UI to inspect the cluster — 2 topics, 51 partitions, 100% preferred partition leader health, 0 under-replicated partitions.

### 3. 🪣 Data Lake — MinIO
- `consumer/kafka-to-minio.py` reads from Kafka and persists JSON files into **MinIO**.
- Files are partitioned by date and hour:
  ```
  spotify/
  └── bronze/
      └── date=2026-06-27/
          └── hour=06/
              ├── spotify_events_2026-06-27T06-57-04.json   (3.1 KiB)
              ├── spotify_events_2026-06-27T06-57-14.json   (3.2 KiB)
              └── ...
  ```
- Bucket: `spotify` | Access: Private | Total: 75.2 KiB across 24 objects

### 4. 🌀 Orchestration — Apache Airflow

**DAG:** `spotify_minio_to_snowflake_bronze`  
**Schedule:** Hourly | **Run time:** ~43 seconds | **Status:** ✅ All runs successful

The DAG contains two sequential tasks:

```
extract_data  ──►  load_raw_to_snowflake
(PythonOperator)   (PythonOperator)
```

| Run | Logical Date | Start | End | Duration |
|---|---|---|---|---|
| ✅ success | 2026-06-27, 10:00 | 11:00:00 | 11:01:39 | ~1m 39s |
| ✅ success | 2026-06-27, 09:00 | 10:00:00 | 10:01:31 | ~1m 31s |
| ✅ success | 2026-06-27, 08:00 | 09:00:01 | 09:01:36 | ~1m 35s |
| ✅ success | 2026-06-27, 07:00 | 08:00:01 | 08:01:43 | ~1m 42s |
| ✅ success | 2026-06-27, 06:00 | 07:00:00 | 07:01:31 | ~1m 31s |
| ✅ success | 2026-06-27, 05:00 | 06:47:47 | 06:48:31 | ~44s |

### 5. ❄️ Data Warehouse — Snowflake

Data lands in the **`SPOTIFY_DB`** database under the **`BRONZE`** schema:

**Table:** `SPOTIFY_EVENTS_BRONZE`  
**Rows loaded:** 50 | **Size:** 5.5 KB | **Role:** `ACCOUNTADMIN` | **Warehouse:** `COMPUTE_WH`

| Column | Sample Values |
|---|---|
| `SONG_NAME` | Love Story, Blinding Lights, Shape of You, God's Plan, Stronger |
| `EVENT_TYPE` | play, pause, skip, add_to_playlist |
| `DEVICE_TYPE` | mobile, web, desktop |
| `COUNTRY` | US, UK, IN, CA, DE |
| `TIMESTAMP` | 2026-06-26T11:55:53.742941Z |

### 6. ⚙️ Data Transformation — dbt

dbt runs transformation models across medallion layers inside **`SPOTIFY_DB`**:

| Layer | Schema | Description |
|---|---|---|
| 🥉 **Bronze** | `BRONZE` | Raw events loaded directly from MinIO via Airflow |
| 🥈 **Silver** | `models/silver/` | Cleaned, deduplicated, and typed records |
| 🥇 **Gold** | `models/gold/` | Aggregated, business-ready models for reporting |

Sources are declared in `spotify_dbt/models/sources.yml`, pointing to the Bronze Snowflake table.

### 7. 📊 Visualization — Power BI
- Gold-layer Snowflake tables are connected to **Power BI** for interactive dashboards.
- Insights: top songs by play count, event type distribution, device breakdown, country-level trends.

---

## 🐳 Docker Services

All services run via Docker. Observed resource usage (4 CPUs, 5.6 GB RAM):

| Container | Image | Port | CPU | Memory |
|---|---|---|---|---|
| `airflow-webserver` | apache/airflow | `8080:8080` | 0.17% | 742 MB |
| `airflow-scheduler` | apache/airflow | — | 5.48% | 437 MB |
| `airflow-postgres` | postgres | `5432:5432` | 1.77% | 68 MB |
| `kafka` | confluentinc/... | `29092:29092` | 0.81% | 415 MB |
| `zookeeper` | confluentinc/... | `2181:2181` | 0.15% | 148 MB |
| `kafdrop` | obsidiandy/... | `9000:9000` | 0.12% | 354 MB |
| `minio` | minio/minio | `9002:9000` | 2.25% | 82 MB |

---

## 🚀 Getting Started

### Prerequisites
- [Docker](https://www.docker.com/) & Docker Compose
- [Python 3.8+](https://www.python.org/)
- [dbt](https://docs.getdbt.com/docs/installation) (`pip install dbt-core dbt-snowflake`)
- Snowflake account (free trial works)
- Power BI Desktop (optional, for dashboard development)

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/spotify-mds-pipeline.git
cd spotify-mds-pipeline
```

### 2. Configure Environment Variables

**Root `.env` (Kafka + MinIO services):**
```env
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET_NAME=spotify

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=spotify-events
```

**`simulator/.env`:**
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=spotify-events
```

**`consumer/.env`:**
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=spotify-events
MINIO_ENDPOINT=localhost:9002
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_NAME=spotify
```

**`docker/.env` (Airflow + Snowflake):**
```env
AIRFLOW__CORE__EXECUTOR=LocalExecutor
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=SPOTIFY_DB
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_NAME=spotify
```

### 3. Start All Services

```bash
# Start Kafka, Zookeeper, MinIO, Kafdrop
docker-compose up -d

# Start Airflow
cd docker/
docker-compose up -d
```

### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 5. Start the Simulator & Consumer

```bash
# Terminal 1 — Stream events into Kafka
python simulator/producer.py

# Terminal 2 — Consume from Kafka and write to MinIO
python consumer/kafka-to-minio.py
```

### 6. Monitor Services

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Kafdrop (Kafka UI) | http://localhost:9000 |
| MinIO Console | http://localhost:9002 |

### 7. Run dbt Transformations

```bash
cd spotify_dbt/
dbt debug          # Verify Snowflake connection
dbt run            # Execute Silver and Gold models
dbt test           # Run data quality tests
dbt docs generate  # Generate documentation
dbt docs serve     # Browse docs at http://localhost:8081
```

### 8. Trigger Airflow DAG

Enable `spotify_minio_to_snowflake_bronze` in the Airflow UI, or trigger manually:

```bash
docker exec -it <airflow-webserver-container> \
  airflow dags trigger spotify_minio_to_snowflake_bronze
```

---

## 📁 Project Structure

```
spotify-mds-pipeline/
├── docker/                         # Dockerized Airflow orchestration environment
│   ├── .env                        # Airflow + Snowflake environment variables
│   ├── docker-compose.yml          # Airflow services (webserver, scheduler, postgres)
│   └── dags/
│       ├── minio-to-kafka.py       # DAG: extract from MinIO → load to Snowflake Bronze
│       └── .env                    # DAG-level environment config
│
├── spotify_dbt/                    # dbt transformation project
│   └── models/
│       ├── gold/                   # Business-ready aggregated models
│       ├── silver/                 # Cleaned and standardized models
│       └── sources.yml             # Source definitions → SPOTIFY_EVENTS_BRONZE
│
├── simulator/                      # Spotify event data simulator
│   ├── producer.py                 # Kafka producer: generates mock Spotify events
│   └── .env                        # Kafka connection config
│
├── consumer/                       # Kafka → MinIO sink
│   ├── kafka-to-minio.py           # Reads Kafka topic, writes partitioned JSON to MinIO
│   └── .env                        # Kafka + MinIO connection config
│
├── docker-compose.yml              # Root compose: Kafka, Zookeeper, MinIO, Kafdrop
├── requirements.txt                # Python dependencies
└── README.md
```

---

## 🧪 Data Quality

dbt tests enforce quality at each layer:
- No duplicate `TIMESTAMP` + `SONG_NAME` combinations
- No null values in `EVENT_TYPE`, `COUNTRY`, `DEVICE_TYPE`
- Valid `EVENT_TYPE` values: `play`, `pause`, `skip`, `add_to_playlist`
- Referential integrity between Silver and Gold models

---

## 🚧 Future Improvements

- [ ] Replace simulator with live Spotify API via OAuth
- [ ] Add dbt incremental models for efficient large-scale loads
- [ ] Implement Great Expectations for advanced data validation
- [ ] Set up CI/CD with GitHub Actions for dbt model testing on PR
- [ ] Add Grafana dashboard for pipeline observability metrics
- [ ] Partition Snowflake tables by date for query performance

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## 🙋 Author

**Your Name**
- GitHub: [@your-username](https://github.com/your-username)
- LinkedIn: [your-linkedin](https://linkedin.com/in/your-linkedin)

---

> Built with ❤️ using Kafka, MinIO, Airflow, Snowflake, dbt, and Power BI.
