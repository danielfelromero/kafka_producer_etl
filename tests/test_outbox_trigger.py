"""Integration test for the SQLite trigger and transactional outbox."""

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class OutboxTriggerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_events.db"
        self.env = {**os.environ, "DB_PATH": str(self.db_path)}
        subprocess.run(
            [sys.executable, str(ROOT / "init_db.py")],
            cwd=ROOT,
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_database_starts_empty(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            records = connection.execute(
                "SELECT COUNT(*) FROM records"
            ).fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM outbox"
            ).fetchone()[0]

        self.assertEqual(records, 0)
        self.assertEqual(events, 0)

    def test_insert_creates_one_pending_outbox_event(self) -> None:
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "insert_record.py"),
                "--station",
                "ST-TEST",
                "--temperature",
                "25.5",
                "--humidity",
                "70",
                "--status",
                "normal",
            ],
            cwd=ROOT,
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
        )

        with sqlite3.connect(self.db_path) as connection:
            record = connection.execute(
                "SELECT record_id, station_id FROM records"
            ).fetchone()
            event = connection.execute(
                """
                SELECT event_id, record_id, event_type, status
                FROM outbox
                """
            ).fetchone()

        self.assertIsNotNone(record)
        self.assertIsNotNone(event)
        self.assertEqual(event[1], record[0])
        self.assertEqual(event[2], "record.created")
        self.assertEqual(event[3], "pending")


if __name__ == "__main__":
    unittest.main()
