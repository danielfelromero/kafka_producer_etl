"""Poll SQLite for new rows and publish each row to Kafka."""

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer

DB_PATH = Path(os.getenv("DB_PATH", "data/events.db"))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", "data/producer_checkpoint.json"))
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


def load_checkpoint() -> int:
    """Load last record ID confirmed by Kafka."""
    if not CHECKPOINT_PATH.exists():
        return 0
    try:
        payload = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        return int(payload.get("last_record_id", 0))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise RuntimeError(f"Cannot read checkpoint {CHECKPOINT_PATH}: {error}") from error


def save_checkpoint(record_id: int) -> None:
    """Persist checkpoint atomically."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = CHECKPOINT_PATH.with_suffix(".tmp")
    temporary.write_text(
        json.dumps({"last_record_id": record_id}, indent=2),
        encoding="utf-8",
    )
    temporary.replace(CHECKPOINT_PATH)


def fetch_new_records(last_record_id: int) -> list[dict[str, Any]]:
    """Fetch append-only rows after the last checkpoint."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found. Run python init_db.py first.")
    with sqlite3.connect(DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT record_id, station_id, temperature, humidity, status, created_at
            FROM records
            WHERE record_id > ?
            ORDER BY record_id ASC
            LIMIT ?
            """,
            (last_record_id, BATCH_SIZE),
        ).fetchall()
    return [dict(row) for row in rows]


def publish_record(producer: Producer, record: dict[str, Any]) -> None:
    """Publish a row and wait for delivery acknowledgement."""
    state: dict[str, Any] = {"done": False, "error": None}

    def on_delivery(error, message) -> None:
        state["done"] = True
        state["error"] = error
        if error is None:
            state["partition"] = message.partition()
            state["offset"] = message.offset()

    event = {**record, "event_type": "record.created"}
    producer.produce(
        TOPIC,
        key=str(record["station_id"]),
        value=json.dumps(event, ensure_ascii=False),
        headers={"event_type": "record.created"},
        callback=on_delivery,
    )

    deadline = time.monotonic() + 30
    while not state["done"] and time.monotonic() < deadline:
        producer.poll(0.2)
    if not state["done"]:
        raise TimeoutError(f"No Kafka acknowledgement for record {record['record_id']}.")
    if state["error"] is not None:
        raise KafkaException(state["error"])

    print(
        f"PUBLISHED record_id={record['record_id']} topic={TOPIC} "
        f"partition={state['partition']} offset={state['offset']}"
    )


def main() -> None:
    if POLL_INTERVAL_SECONDS <= 0 or BATCH_SIZE < 1:
        raise ValueError("Polling interval and batch size must be positive.")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "sqlite-change-publisher",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    last_record_id = load_checkpoint()
    print(f"Watching {DB_PATH}; checkpoint={last_record_id}")
    print(f"Kafka bootstrap={BOOTSTRAP_SERVERS}; topic={TOPIC}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            records = fetch_new_records(last_record_id)
            if not records:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for record in records:
                # Advance only after Kafka confirms delivery.
                publish_record(producer, record)
                last_record_id = int(record["record_id"])
                save_checkpoint(last_record_id)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        outstanding = producer.flush(10)
        if outstanding:
            print(f"Warning: {outstanding} message(s) remain undelivered.")


if __name__ == "__main__":
    main()
