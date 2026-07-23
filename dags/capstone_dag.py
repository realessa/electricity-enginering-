# -*- coding: utf-8 -*-
"""
Airflow DAG for the electricity capstone pipeline.
Dependency chain: ingest -> bronze_silver -> quality_gate -> [gold, rag]
If quality_gate raises QualityGateFailure, Airflow marks it failed and
gold/rag (its downstream tasks) never run.

Place this file in your Airflow dags/ folder, and make sure
capstone_pipeline/ is importable (adjust the sys.path line below to your setup).
"""

import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/project") # <-- adjust to your project path

from dags.producer import run_producer
from dags.consumer import run_consumer
from dags.lakehouse import get_spark, build_bronze, prove_schema_enforcement, build_silver, build_gold
from dags.quality_gate import run_quality_gate, QualityGateFailure
from dags.lineage_utils import emit_event
from openlineage.client.run import RunState

default_args = {"owner": "capstone", "retries": 0}


def task_ingest(**context):
    emit_event("ingestion", RunState.START)

    run_producer(
        bootstrap_servers="host.docker.internal:9092"
    )

    consumed = run_consumer(
        bootstrap_servers="host.docker.internal:9092",
        max_messages=4,
        timeout_ms=10000
    )

    print("XCOM DATA:", consumed)

    context["ti"].xcom_push(
        key="consumed_records",
        value=consumed
    )

    emit_event("ingestion", RunState.COMPLETE)

    return consumed

def task_bronze_silver(**context):
    emit_event("bronze_silver", RunState.START)

    consumed = context["ti"].xcom_pull(
        task_ids="ingest",
        key="consumed_records"
    )

    print("RECEIVED FROM XCOM:", consumed)

    if consumed is None:
        raise ValueError(
            "No XCom data. Run ingest task first."
        )

    spark = get_spark()

    try:
        bronze_df = build_bronze(
            spark,
            consumed
        )

        prove_schema_enforcement(spark)

        build_silver(
            spark,
            bronze_df
        )

        emit_event("bronze_silver", RunState.COMPLETE)

    except Exception:
        emit_event("bronze_silver", RunState.FAIL)
        raise


def task_quality_gate(**context):
    emit_event("quality_gate", RunState.START)
    spark = get_spark()
    silver_df = spark.read.format("delta").load("silver")
    try:
        run_quality_gate(silver_df)
        emit_event("quality_gate", RunState.COMPLETE)
    except QualityGateFailure:
        emit_event("quality_gate", RunState.FAIL)
        raise  # propagate so Airflow halts gold/rag


def task_gold(**context):
    emit_event("gold", RunState.START)
    spark = get_spark()
    silver_df = spark.read.format("delta").load("silver")
    build_gold(spark, silver_df)
    emit_event("gold", RunState.COMPLETE)


def task_rag(**context):
    emit_event("rag", RunState.START)
    from dags.rag_pipeline import run_rag
    run_rag("What are the benefits of smart meters?")
    emit_event("rag", RunState.COMPLETE)


with DAG(
    dag_id="electricity_capstone_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["capstone", "sdaia"],
) as dag:

    ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)
    bronze_silver = PythonOperator(task_id="bronze_silver", python_callable=task_bronze_silver)
    quality_gate = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    gold = PythonOperator(task_id="gold", python_callable=task_gold)
    rag = PythonOperator(task_id="rag", python_callable=task_rag)

    ingest >> bronze_silver >> quality_gate >> [gold, rag]
