# Architecture and teaching notes

## Event flow

1. A source application inserts a new row into `records`.
2. A SQLite `AFTER INSERT` trigger creates a `pending` event in `outbox`.
3. The trigger and original insert execute within the same database transaction.
4. The Python producer polls the outbox for pending events.
5. The producer serializes the event and publishes it to a Kafka topic.
6. After Kafka confirms delivery, the producer marks the outbox event as `published`.
7. Independent consumers receive the event and implement downstream processing.

## Pattern: Transactional Outbox

The outbox avoids a common dual-write problem: an application should not commit a business row and then independently try to publish a Kafka event without a durable record of the publication intent. The database trigger records that intent in the same transaction as the business insert.

The producer process is not started directly by the SQLite trigger. Instead, it continuously polls the outbox. This is a simple way to demonstrate the outbox pattern without adding Debezium or a database server.

## Why SQLite

- No database server, user management, or credentials are needed.
- A newly initialized database contains zero business rows.
- Students can inspect the source table and outbox table.
- The exercise focuses on event publication and downstream consumers.

## Delivery semantics

The producer marks an outbox row as published only after Kafka confirms delivery. If it crashes after Kafka accepted the message but before the SQLite status update, the message can be published again after restart. Therefore this example provides **at-least-once delivery**, not end-to-end exactly-once semantics.

The `event_id` is stable and can be used by consumers for deduplication. Kafka producer idempotence helps with retries within a producer session, but it does not remove the database/Kafka dual-write boundary.

This starter assumes one producer instance. Multiple concurrent producers would need an atomic claim/lease strategy for pending outbox rows.

## Polling vs. CDC

The producer checks the outbox every `POLL_INTERVAL_SECONDS`. This introduces bounded detection latency and repeated database queries. The SQLite trigger is database-level change handling, but the publisher still uses polling to discover work.

For larger production systems, compare this design with CDC tools such as Debezium reading a database transaction log. The choice involves operational complexity, latency, throughput, and coupling to the database engine.

## Discussion questions

1. What does the SQLite trigger do, and what does the Kafka producer do?
2. Why is the outbox row created in the same transaction as the business row?
3. What happens if Kafka is unavailable when a record is inserted?
4. Why can a duplicate event occur after a crash?
5. How can a consumer make processing idempotent using `event_id`?
6. What is the effect of reducing or increasing the polling interval?
7. What additional mechanism is needed for multiple producer instances?
