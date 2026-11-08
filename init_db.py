"""Create an empty SQLite database with a transactional outbox trigger."""

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "data/events.db"))


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('normal', 'alert')),
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS outbox (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id INTEGER NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'published')),
                published_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                FOREIGN KEY (record_id) REFERENCES records(record_id)
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_status_event_id
                ON outbox(status, event_id);

            CREATE TRIGGER IF NOT EXISTS trg_records_after_insert
            AFTER INSERT ON records
            BEGIN
                INSERT INTO outbox (record_id, event_type, created_at)
                VALUES (NEW.record_id, 'record.created', NEW.created_at);
            END;
            """
        )
        connection.commit()

        records_count = connection.execute(
            "SELECT COUNT(*) FROM records"
        ).fetchone()[0]
        outbox_count = connection.execute(
            "SELECT COUNT(*) FROM outbox"
        ).fetchone()[0]

    print(f"Database initialized: {DB_PATH}")
    print("Tables 'records' and 'outbox' and the insert trigger are ready.")
    print(f"Current rows — records: {records_count}, outbox: {outbox_count}")
    if records_count or outbox_count:
        print("Note: the database already contained data; it was not emptied.")


if __name__ == "__main__":
    main()
