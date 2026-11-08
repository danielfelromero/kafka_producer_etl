"""Insert one business record into the SQLite source database."""

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "data/events.db"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Insert a measurement record.")
    parser.add_argument("--station", help="Station identifier, e.g. ST-01")
    parser.add_argument("--temperature", type=float, help="Temperature in Celsius")
    parser.add_argument("--humidity", type=float, help="Relative humidity (0-100)")
    parser.add_argument("--status", choices=["normal", "alert"], help="Record status")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    station = args.station or input("Station ID [ST-01]: ").strip() or "ST-01"
    temperature = args.temperature
    if temperature is None:
        temperature = float(input("Temperature (C): "))
    humidity = args.humidity
    if humidity is None:
        humidity = float(input("Humidity (0-100): "))
    status = args.status or input("Status (normal/alert) [normal]: ").strip() or "normal"

    if not 0 <= humidity <= 100:
        raise ValueError("Humidity must be between 0 and 100.")
    if status not in {"normal", "alert"}:
        raise ValueError("Status must be 'normal' or 'alert'.")
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. Run 'python init_db.py' first."
        )

    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.execute(
            """
            INSERT INTO records
                (station_id, temperature, humidity, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (station, temperature, humidity, status, created_at),
        )
        record_id = cursor.lastrowid
        connection.commit()

    print(f"Inserted record_id={record_id} into {DB_PATH}.")


if __name__ == "__main__":
    main()
