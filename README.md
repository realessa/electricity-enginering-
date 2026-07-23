# Electricity Data Engineering Pipeline

**Trainee:** Reema Alessa

**Training Program:** Modern Data Engineering for AI Systems – SDAIA Academy

**Project Duration:** 19-07-2026 → 23-07-2026

---

# Project Overview

This project demonstrates an end-to-end modern data engineering pipeline for electricity meter readings.

The pipeline simulates streaming electricity consumption data using Apache Kafka, processes it with Apache Spark and Delta Lake, validates data quality before loading trusted data, and integrates a Retrieval-Augmented Generation (RAG) component for querying the processed data.

The project was developed as the Capstone Project for the SDAIA Academy training program.

SDAIA Academy GitHub:
https://github.com/SDAIAAcademy

---

# Features

- Kafka Producer & Consumer
- Bronze → Silver → Gold Lakehouse architecture
- Delta Lake storage
- Schema enforcement
- Data Quality Gate
- Dead Letter Queue (DLQ)
- Airflow orchestration
- OpenLineage integration
- RAG pipeline
- End-to-end execution

---

# Pipeline Architecture

```
Producer
    │
    ▼
Kafka Topic
    │
    ▼
Consumer
    │
    ▼
Bronze Layer
    │
    ▼
Schema Enforcement
    │
    ▼
Silver Layer
    │
    ▼
Quality Gate
    │
 ┌──┴──┐
 ▼     ▼
Gold   RAG
```

---

# Technologies Used

- Python
- Apache Kafka
- Apache Airflow
- Apache Spark
- Delta Lake
- OpenLineage
- ChromaDB
- Sentence Transformers
- Docker
- Docker Compose

---

# Project Structure

# Project Structure

```text
electricity-pj/
│
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .gitignore
│
├── dags/
│   ├── capstone_dag.py
│   ├── main.py
│   ├── producer.py
│   ├── consumer.py
│   ├── lakehouse.py
│   ├── quality_gate.py
│   ├── rag_pipeline.py
│   ├── lineage_utils.py
│   ├── schemas.py
│   └── Global Energy Management Guide.pdf
│
├── evidence/
│   ├── run prof.txt
│   └── screenair.png
```

---

# Installation

Clone the repository:

```bash
git clone <repository-url>
cd electricity-pj
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Start Docker containers:

```bash
docker compose up -d
```

---

# Running the Project

Run the Airflow pipeline:

```bash
airflow dags trigger electricity_capstone_pipeline
```

Or execute the pipeline manually:

```bash
python -m dags.main
```

---

# Data Validation

The project validates data before processing.

Accepted records continue through the pipeline.

Malformed or invalid records are redirected to a Dead Letter Queue (DLQ).

Schema enforcement prevents invalid writes to the Delta Lake.

The Quality Gate stops downstream processing whenever data quality checks fail.

---

# Evidence

The **evidence** folder contains proof that the pipeline was executed successfully.

Included evidence:

- **run prof.txt** — Console output showing successful execution of Kafka, Spark, and Airflow tasks.
- **screenair.png** — Screenshot showing a successful Airflow DAG execution.

The project also demonstrates failure scenarios including:

- Rejected malformed Kafka records
- Schema enforcement preventing invalid writes
- Quality Gate blocking downstream tasks when validation fails


---

# Future Improvements

- Real-time dashboard
- Multiple Kafka partitions
- Cloud deployment
- Automated monitoring
- CI/CD pipeline

---

# Acknowledgment

This project was completed as part of the **Modern Data Engineering for AI Systems** training program at **SDAIA Academy**.

https://github.com/SDAIAAcademy

---

# License

This project is intended for educational purposes only.
