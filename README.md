# NYC Taxi Big Data Pipeline

A containerized Big Data pipeline built with **PySpark**, **Apache Hive**, and a live **Flask** web dashboard — all running inside Docker so every team member gets an identical environment with a single command.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Environment | Docker Compose | Consistent runtime for the whole team |
| Storage | Local `./dataset/` folder | Raw Parquet files (mounted into Spark & Hive) |
| ETL | PySpark 3.5 | Extract, clean, enrich, and load data |
| Data Warehouse | Apache Hive 3.1 | Queryable SQL tables for KPI results |
| Metastore | PostgreSQL 13 | Hive schema/metadata storage (required for JDBC driver compatibility) |
| Dashboard | Flask & Plotly.js (Python 3.10) | Live interactive HTML dashboard re-rendered on page load |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and **running**
- Git

---

## Quick Start

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd big-Data-project
```

### 2. Add your raw data
Place your `.parquet` dataset files inside the `./dataset/` folder (this folder is ignored by Git):
```
big-Data-project/
└── dataset/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── yellow_tripdata_2025-03.parquet
```

### 3. Initialize the Metastore & Start Containers
Because multiple services access the PostgreSQL database, we initialize the Hive Metastore database schema officially using the `schematool` container one-off runner first. This prevents concurrent write deadlock errors:

```bash
# 1. Start the PostgreSQL database
docker compose up -d postgres

# 2. Run the metastore schema initialization tool
docker compose run --rm --entrypoint /opt/hive/bin/schematool hive-metastore -dbType postgres -initSchema

# 3. Start all other containers in the background
docker compose up -d
```

### 4. Create the Hive Tables
Run the Hive DDL script inside the HiveServer container to set up the clean tables:
```bash
docker exec nychiveserver beeline \
  -u "jdbc:hive2://localhost:10000/;auth=noSasl" -n hive \
  -f /database_scripts/create_tables.hql
```

### 5. Run the Spark ETL Job
Submit the PySpark pipeline script to the cluster. This will clean the raw data and write it directly into the Hive tables:
```bash
docker exec nycsparkmaster /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/hive/warehouse \
  /app/transform.py
```

### 6. Open the UIs

| UI | URL | Description |
|---|---|---|
| **Flask Dashboard** | http://localhost:8501 | Live dashboard (queries Hive on page load) |
| **Spark Master UI** | http://localhost:8090 | Spark cluster master dashboard |
| **Spark Worker UI** | http://localhost:8081 | Spark cluster worker executor stats |
| **HiveServer2 Web UI** | http://localhost:10002 | HiveServer2 status dashboard |

---

## Project Structure

```
big-Data-project/
├── .gitignore               # Ignores dataset/, .env, *.csv, *.parquet etc.
├── README.md                # This file
├── docker-compose.yml       # All container definitions
│
├── dataset/                 # ← Drop your Parquet files here (Git-ignored)
│
├── spark_job/               # Spark / PySpark scripts
│   └── transform.py         # Main ETL pipeline script
│
├── hive_setup/              # Hive SQL configs and scripts
│   ├── hive-site.xml        # Shared Hive/metastore configuration
│   └── create_tables.hql    # DDL: creates all Hive tables
│
└── python_app/              # Flask Web Server
    ├── Dockerfile
    ├── main.py              # Flask server, queries Hive and packages Plotly JSON
    ├── requirements.txt     # Python dependencies (pyhive, flask, etc.)
    └── templates/
        └── dashboard.html   # Dark glassmorphism Jinja2 template with Plotly.js charts
```

---

## Networking Rules (IMPORTANT for teammates)

Inside Docker, **never use `localhost`** to connect services to each other. Use the **service name** defined in `docker-compose.yml` instead:

| Connection | Use this hostname |
|---|---|
| Flask → Hive | `hive-server` |
| Spark → Hive Metastore | `hive-metastore` |
| Hive Metastore → PostgreSQL | `postgres` |
| Spark Worker → Master | `spark-master` |

**Example (PyHive in Python):**
```python
from pyhive import hive

conn = hive.connect(
    host="hive-server",   # ← service name, not localhost
    port=10000,
    database="nyc_taxi",
    auth="NOSASL"
)
```

---

## Stopping & Resetting

```bash
# Stop all containers (data is preserved in volumes)
docker compose down

# Stop AND delete all stored volumes (fresh start — requires running schematool again)
docker compose down -v
```
