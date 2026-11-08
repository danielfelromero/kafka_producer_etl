"""Create an empty SQLite database for the Kafka producer lab."""

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "data/events.db"))


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.commit()

    print(f"Database initialized: {DB_PATH}")
    print("Table 'records' is ready; no business records were inserted.")


if __name__ == "__main__":
    main()
