import json
import os
from kafka import KafkaConsumer

MAIN_TOPIC = "electricity-data"




def run_consumer(
    topic=MAIN_TOPIC,
    bootstrap_servers="host.docker.internal:9092",
    max_messages=None,
    timeout_ms=10000
):

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )

    consumed = []

    for message in consumer:
        consumed.append(message.value)

        print(
            f"Consumed: {message.value}"
        )

        if max_messages and len(consumed) >= max_messages:
            break

    consumer.close()

    print(f"Total consumed: {len(consumed)}")

    return consumed
