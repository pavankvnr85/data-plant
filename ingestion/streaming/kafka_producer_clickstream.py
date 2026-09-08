"""
Generates synthetic clickstream events onto a Kafka/Redpanda topic.

Local dev: run `docker compose up -d` first, then this script against
localhost:19092 (Redpanda's exposed port in docker-compose.yml).

This exists purely to give the streaming layer something real to process
without needing a live production app — swap this for a real event source
later without touching the Structured Streaming job downstream.
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "clickstream"
BOOTSTRAP_SERVERS = ["localhost:19092"]

PAGES = ["/home", "/product/1", "/product/2", "/cart", "/checkout", "/search"]
EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase"]


def make_event(session_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": f"user_{random.randint(1, 500)}",
        "event_type": random.choices(EVENT_TYPES, weights=[60, 25, 10, 5])[0],
        "page": random.choice(PAGES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main(events_per_second: int = 5):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sessions = [str(uuid.uuid4()) for _ in range(20)]

    print(f"Producing to '{TOPIC}' at ~{events_per_second}/s. Ctrl+C to stop.")
    try:
        while True:
            for _ in range(events_per_second):
                event = make_event(random.choice(sessions))
                producer.send(TOPIC, value=event)
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
