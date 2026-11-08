"""Poll the SQLite transactional outbox and publish pending events to Kafka."""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

DB_PATH = Path(os.getenv("DB_PATH", "data/events.db"))
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


def fetch_pending_events() -> list[dict[str, Any]]:
    """Read pending outbox events together with their source record."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found. Run python init_db.py first.")

    with sqlite3.connect(DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT
                o.event_id,
                o.record_id,
                o.event_type,
                o.created_at AS event_created_at,
                r.station_id,
                r.temperature,
                r.humidity,
                r.status,
                r.created_at
            FROM outbox AS o
            INNER JOIN records AS r ON r.record_id = o.record_id
            WHERE o.status = 'pending'
            ORDER BY o.event_id ASC
            LIMIT ?
            """,
            (BATCH_SIZE,),
        ).fetchall()

    return [dict(row) for row in rows]


def mark_published(event_id: int) -> None:
    """Mark an event published only after Kafka acknowledges delivery."""
    published_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.execute(
            """
            UPDATE outbox
            SET status = 'published',
                published_at = ?,
                attempts = attempts + 1,
                last_error = NULL
            WHERE event_id = ? AND status = 'pending'
            """,
            (published_at, event_id),
        )
        connection.commit()
        if cursor.rowcount != 1:
            raise RuntimeError(
                f"Outbox event {event_id} was not marked published. "
                "Check whether another producer changed its status."
            )


def mark_attempt_failed(event_id: int, error: Exception) -> None:
    """Keep the event pending and record the failed publication attempt."""
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            UPDATE outbox
            SET attempts = attempts + 1,
                last_error = ?
            WHERE event_id = ? AND status = 'pending'
            """,
            (str(error)[:1000], event_id),
        )
        connection.commit()


def publish_event(producer: Producer, event: dict[str, Any]) -> None:
    """Publish one outbox event and wait for its delivery callback."""
    delivery: dict[str, Any] = {"completed": False, "error": None}

    def on_delivery(error, message) -> None:
        delivery["completed"] = True
        delivery["error"] = error
        if error is None:
            delivery["partition"] = message.partition()
            delivery["offset"] = message.offset()

    payload = {
        "event_id": event["event_id"],
        "record_id": event["record_id"],
        "event_type": event["event_type"],
        "station_id": event["station_id"],
        "temperature": event["temperature"],
        "humidity": event["humidity"],
        "status": event["status"],
        "created_at": event["created_at"],
    }

    producer.produce(
        TOPIC,
        key=str(event["station_id"]),
        value=json.dumps(payload, ensure_ascii=False),
        headers=[
            ("event_type", str(event["event_type"]).encode("utf-8")),
            ("event_id", str(event["event_id"]).encode("utf-8")),
        ],
        callback=on_delivery,
    )

    deadline = time.monotonic() + 30
    while not delivery["completed"] and time.monotonic() < deadline:
        producer.poll(0.2)

    if not delivery["completed"]:
        raise TimeoutError(
            f"No delivery acknowledgement for outbox event {event['event_id']}."
        )
    if delivery["error"] is not None:
        raise RuntimeError(
            f"Kafka delivery failed for event {event['event_id']}: "
            f"{delivery['error']}"
        )

    print(
        f"PUBLISHED event_id={event['event_id']} "
        f"record_id={event['record_id']} topic={TOPIC} "
        f"partition={delivery['partition']} offset={delivery['offset']}"
    )


def main() -> None:
    if POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be greater than zero.")
    if BATCH_SIZE < 1:
        raise ValueError("BATCH_SIZE must be at least 1.")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found. Run python init_db.py first.")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "sqlite-outbox-kafka-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    print(f"Watching SQLite outbox: {DB_PATH}")
    print(f"Kafka bootstrap: {BOOTSTRAP_SERVERS} | topic: {TOPIC}")
    print("Waiting for pending events. Press Ctrl+C to stop.")

    try:
        while True:
            events = fetch_pending_events()
            if not events:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for event in events:
                try:
                    publish_event(producer, event)
                    mark_published(int(event["event_id"]))
                except Exception as error:
                    mark_attempt_failed(int(event["event_id"]), error)
                    print(
                        f"ERROR event_id={event['event_id']}: {error}. "
                        f"The event remains pending and will be retried."
                    )
                    # Avoid repeatedly hammering Kafka when a delivery fails.
                    time.sleep(POLL_INTERVAL_SECONDS)
                    break

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        outstanding = producer.flush(10)
        if outstanding:
            print(f"Warning: {outstanding} message(s) remain undelivered.")


if __name__ == "__main__":
    main()
