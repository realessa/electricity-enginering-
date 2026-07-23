from dags.producer import run_producer
from dags.consumer import run_consumer
from dags.lakehouse import run_lakehouse
from dags.quality_gate import run_quality_gate
from dags.rag_pipeline import run_rag


def main():

    print("[START] Starting pipeline")

    # 1- Kafka Producer
    run_producer()

    # 2- Kafka Consumer
    records = run_consumer()

    # 3- Spark + Delta Lake
    spark, bronze, silver, gold = run_lakehouse(records)

    # 4- Quality Check
    run_quality_gate(silver)

    print("[RAG] Running RAG...")
    run_rag("What are the benefits of smart meters?")

    print("[SUCCESS] Pipeline Finished Successfully")


if __name__ == "__main__":
    main()