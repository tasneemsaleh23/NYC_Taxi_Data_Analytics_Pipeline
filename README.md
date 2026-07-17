# NYC Taxi Big Data Pipeline

A containerized Big Data pipeline built with **PySpark**, **Apache Hive**, and **Streamlit** — all running inside Docker so every team member gets an identical environment with a single command.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Environment | Docker Compose | Consistent runtime for the whole team |
| Storage | Local `./data/` folder | Raw Parquet files (mounted into Spark) |
| ETL | PySpark 3.5 | Extract, clean, enrich, and load data |
| Data Warehouse | Apache Hive 3.1 | Queryable SQL tables for KPI results |
| Metastore | PostgreSQL 15 | Hive schema/metadata storage |
| Dashboard | Streamlit (Python 3.11) | Interactive KPI visualisation |

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
Place your `.parquet` files inside the `./data/` folder (this folder is ignored by Git):
```
big-Data-project/
└── data/
    ├── yellow_tripdata_2024-01.parquet
    ├── yellow_tripdata_2024-02.parquet
    └── yellow_tripdata_2024-03.parquet
```

### 3. Start all containers
```bash
docker compose up -d
```
> First run will pull images — this takes a few minutes. Subsequent starts are instant.

### 4. Open the UIs

| UI | URL |
|---|---|
| **Streamlit Dashboard** | http://localhost:8501 |
| **Spark Master UI** | http://localhost:8080 |
| **Spark Worker UI** | http://localhost:8081 |
| **HiveServer2 Web UI** | http://localhost:10002 |

---

## Project Structure

```
big-Data-project/
├── .gitignore               # Ignores data/, .env, *.csv, *.parquet etc.
├── README.md                # This file
├── docker-compose.yml       # All container definitions
│
├── data/                    # ← Drop your Parquet files here (Git-ignored)
│
├── spark_job/               # Spark / PySpark scripts
│   ├── .gitkeep
│   └── transform.py         # Main ETL pipeline script
│
├── hive_setup/              # Hive SQL scripts
│   ├── .gitkeep
│   └── create_tables.hql    # DDL: creates all Hive tables
│
└── streamlit_app/           # Streamlit dashboard
    ├── .gitkeep
    ├── app.py               # Dashboard entry point
    └── requirements.txt     # Python dependencies (pyhive, streamlit, etc.)
```

---

## Networking Rules (IMPORTANT for teammates)

Inside Docker, **never use `localhost`** to connect services to each other.  
Use the **service name** defined in `docker-compose.yml` instead:

| Connection | Use this hostname |
|---|---|
| Streamlit → Hive | `hive-server` |
| Spark → Hive | `hive-server` |
| Hive → PostgreSQL | `postgres` |
| Spark Worker → Master | `spark-master` |

**Example (PyHive in Python):**
```python
from pyhive import hive

conn = hive.Connection(
    host="hive-server",   # ← service name, not localhost
    port=10000,
    username="hive"
)
```

---

## Stopping & Resetting

```bash
# Stop all containers (data is preserved)
docker compose down

# Stop AND delete all stored data (fresh start)
docker compose down -v
```

---

## Running the Spark Job

```bash
# Submit your PySpark transform.py to the cluster
docker exec nyc_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /app/transform.py
```

## Running the Hive DDL Scripts

```bash
# Execute your create_tables.hql inside the Hive container
docker exec nyc_hive beeline \
  -u "jdbc:hive2://localhost:10000" \
  -f /database_scripts/create_tables.hql
```
