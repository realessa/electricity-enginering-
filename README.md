# Electricity Capstone Data Pipeline

An end-to-end data engineering pipeline for electricity meter readings, built for the **Modern Data Engineering for AI Systems** capstone. The pipeline streams meter data through Kafka, validates and lands it in a Delta Lake (Bronze/Silver/Gold), gates it with automated quality checks, orchestrates every stage with Apache Airflow, emits OpenLineage events per stage, and powers a Retrieval-Augmented Generation (RAG) system for answering questions over a supporting energy-management document.

> **Trainee:** Reema Sami Alessa
> **Training Program:** Modern Data Engineering for AI Systems — SDAIA Academy (delivered via Learning Space)
> **Trainer:** Mohammed Albeladi
> **Cohort / Session Dates:** _<fill in your cohort dates here>_
> Reference: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)

---

# Smart Electricity Meter Data Pipeline

## Project Description

This project implements a modern data engineering pipeline for smart electricity meter data.

The system simulates electricity meter readings, streams data using Apache Kafka, processes and stores data using Apache Spark and Delta Lake, validates data quality, creates analytical data, and provides a simple RAG-based question answering component.

The goal of this project is to demonstrate an end-to-end data pipeline for AI and analytics systems.

---

# Project Architecture

```
Smart Meter Data
        |
        v
Kafka Producer
        |
        v
Kafka Topic
        |
        v
Airflow Pipeline
        |
        v
Bronze Layer
        |
        v
Silver Layer
        |
        v
Quality Gate
        |
   +----+----+
   |         |
   v         v
 Gold       RAG
 Analytics  Question Answering
```

---

# Technologies Used

* Python
* Apache Kafka
* Apache Airflow
* Apache Spark
* Delta Lake
* Docker
* OpenLineage

---

# Pipeline Workflow

The pipeline consists of the following stages:

## 1. Data Ingestion

* A Kafka producer generates smart meter readings.
* Data is sent to the electricity data topic.
* Kafka consumer retrieves the messages for processing.

Example data:

```json
{
  "meter_id": "M001",
  "building": "Building_A",
  "consumption_kwh": 18.5,
  "timestamp": "2026-07-23T02:23:38"
}
```

---

## 2. Bronze Layer

The Bronze layer stores the raw incoming data without major transformations.

Purpose:

* Preserve original data.
* Provide a raw data history.
* Support future reprocessing.

---

## 3. Silver Layer

The Silver layer cleans and prepares the data.

Operations include:

* Removing duplicate records.
* Validating values.
* Applying schema rules.

---

## 4. Quality Gate

Before creating analytical data, the pipeline checks data quality.

Examples of rejected data:

* Negative electricity consumption.
* Invalid records.
* Schema mismatch.

If the quality check fails, downstream tasks stop.

---

## 5. Gold Layer

The Gold layer contains processed analytical data.

Examples:

* Average electricity consumption per building.
* Aggregated meter statistics.

---

## 6. RAG Component

The RAG pipeline provides a simple question-answering layer using processed information.

Example:

Question:

```
What are the benefits of smart meters?
```

Response:

```
Smart meters provide real-time electricity monitoring,
help reduce energy waste, and improve consumption analysis.
```

---

# Project Structure

```
electricity-capstone/

│
├── README.md
├── docker-compose.yml
├── requirements.txt
├── .gitignore
│
├── dags/
│   ├── capstone_dag.py
│   ├── producer.py
│   ├── consumer.py
│   ├── lakehouse.py
│   ├── quality_gate.py
│   └── rag_pipeline.py
│
├── evidence/
│   ├── run prof.txt
│   ├── screenair.png
```

---

# How to Run

## 1. Start Docker Services

```bash
docker compose up -d
```

---

## 2. Access Airflow Container

```bash
docker exec -it electricity-pj-airflow-1 bash
```

---

## 3. Run the Pipeline

```bash
airflow dags test electricity_capstone_pipeline 2026-07-23
```

---

# Expected Output

Successful execution should show:

```
ingest SUCCESS

bronze_silver SUCCESS

quality_gate SUCCESS

gold SUCCESS

rag SUCCESS
```

---

# Failure Testing

The project includes failure path validation.

## Invalid Record Example

Input:

```
meter_id=M006
consumption_kwh=-50
```

Result:

```
Rejected record
Quality validation failed
```

---

## Schema Enforcement Test

An invalid data type is attempted to be written.

Example:

```
consumption_kwh = "wrong_type"
```

Expected result:

```
Write rejected because of schema enforcement.
```

---

# Evidence

Execution logs and validation results are stored in:

```
evidence/
```

Including:

* Successful Airflow pipeline execution.
* Invalid record rejection.
* Schema enforcement failure.

---

# Environment Requirements

Before running the project, install:

* Docker
* Python 3.11+
* Apache Kafka
* Apache Spark
* Apache Airflow

---

# Training Program

This project was completed as part of:

**Modern Data Engineering for AI Systems**

Training Program by:

SDAIA Academy

GitHub:
https://github.com/SDAIAAcademy

# Anowleadg 


This project was completed as part of the **Modern Data Engineering for AI Systems** program delivered by **SDAIA Academy** via Learning Space. See [github.com/SDAIAAcademy](https://github.com/SDAIAAcademy) for more on the program.
