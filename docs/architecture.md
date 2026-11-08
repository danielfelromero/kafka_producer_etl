# Architecture and teaching notes

## Why database insertion is the trigger

1. A source application inserts a row into SQLite.
2. The producer polls for rows with IDs greater than its checkpoint.
3. The row is serialized as a `record.created` event.
4. Kafka stores the event in a topic.
5. Independent consumers subscribe and implement downstream processing.

## Polling is not native CDC

The insert does not call Kafka directly. The producer detects it during its next polling iteration. This is **polling-based change detection**, not push notification and not CDC from a transaction log.

## Why SQLite

- No database server or credentials are required.
- The initial database has zero business rows.
- Students can inspect the data directly.
- The class can focus on the event production pattern.

## Limitations and discussion questions

1. What is the trade-off between polling interval, latency, and database load?
2. What happens if the producer is offline while rows are inserted?
3. Why is a persistent checkpoint required?
4. Can an event be published twice after a crash?
5. How would CDC with Debezium differ?
6. What if rows can be updated or deleted?
7. What should happen when Kafka is unavailable?

The checkpoint is a high-water mark on an autoincrementing ID. It suits an introductory append-only exercise, not a complete production CDC platform.
