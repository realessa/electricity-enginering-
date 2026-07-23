import json
import os
from datetime import datetime
from kafka import KafkaProducer
from dags.schemas import ElectricityReading


MAIN_TOPIC = "electricity-data"
DLQ_TOPIC = "electricity-dead-letter"


electricity_data = [
    {
        "meter_id": "M001",
        "building": "Building_A",
        "consumption_kwh": 18.5,
        "timestamp": datetime.utcnow()
    },
    {
        "meter_id": "M002",
        "building": "Building_B",
        "consumption_kwh": 22.1,
        "timestamp": datetime.utcnow()
    },
    {
        "meter_id": "",
        "building": "Building_E",
        "consumption_kwh": 14.2,
        "timestamp": datetime.utcnow()
    },
    {
        "meter_id": "M006",
        "building": "Building_F",
        "consumption_kwh": -8,
        "timestamp": datetime.utcnow()
    }
]





def run_producer(records=None, bootstrap_servers="host.docker.internal:9092"):

    if bootstrap_servers is None:
        bootstrap_servers = os.getenv(
            "KAFKA_SERVER",
            "kafka:9092"
        )


    records = records or electricity_data

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x:
            json.dumps(x, default=str).encode("utf-8")
    )


    valid = 0
    rejected = 0


    for record in records:

        try:
            reading = ElectricityReading(**record)


            producer.send(
                MAIN_TOPIC,
                key=reading.meter_id.encode(),
                value=reading.model_dump(mode="json")
            )

            print(
                f"✅ Sent {reading.meter_id}"
            )

            valid += 1


        except Exception as e:

            dlq_record = {
                "data": record,
                "error": str(e),
                "failed_at": datetime.utcnow()
            }


            producer.send(
                DLQ_TOPIC,
                value=dlq_record
            )

            print(
                f"❌ Rejected {record.get('meter_id')}"
            )

            rejected += 1


    producer.flush()


    print(
        f"\nAccepted: {valid} | Rejected: {rejected}"
    )


if __name__ == "__main__":
    run_producer()
