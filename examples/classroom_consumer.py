"""Consumer de ejemplo para estudiantes conectados al broker del docente.

El docente debe compartir el endpoint TCP mostrado por:
  cat .kafka/ngrok_tcp_address

Cada estudiante debe utilizar un group.id diferente si todos necesitan
recibir independientemente los mismos eventos.
"""

import json
import os
import sys

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "student-change-this")
TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")

if not BOOTSTRAP_SERVERS:
    raise SystemExit(
        "Define KAFKA_BOOTSTRAP_SERVERS con el host:puerto TCP asignado por ngrok."
    )

if not CONSUMER_GROUP.startswith("student-"):
    raise SystemExit("Usa un CONSUMER_GROUP que comience por 'student-'.")

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "PLAINTEXT",
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
)

try:
    metadata = consumer.list_topics(timeout=20)
    print("Conectado a Kafka.")
    print("Cluster ID:", metadata.cluster_id)
    print("Brokers anunciados:", {
        broker_id: f"{broker.host}:{broker.port}"
        for broker_id, broker in metadata.brokers.items()
    })

    consumer.subscribe([TOPIC])
    print(f"Suscrito a {TOPIC}. Esperando eventos...")

    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print("Error de Kafka:", message.error(), file=sys.stderr)
            continue

        raw_value = message.value().decode("utf-8")
        try:
            event = json.loads(raw_value)
        except json.JSONDecodeError:
            event = raw_value

        print(
            f"topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()} "
            f"key={message.key()!r}"
        )
        print("Evento:", event)
        consumer.commit(message=message, asynchronous=False)

except KeyboardInterrupt:
    print("\nConsumer detenido por el usuario.")
except KafkaException as exc:
    print(f"Error de Kafka: {exc}", file=sys.stderr)
    raise
finally:
    consumer.close()
