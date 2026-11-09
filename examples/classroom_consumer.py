"""Consumer de ejemplo para estudiantes conectados al broker del docente.

Variables de entorno requeridas:
  KAFKA_BOOTSTRAP_SERVERS: dominio público ngrok, por ejemplo my-domain.ngrok.app:443
  KAFKA_USERNAME: student
  KAFKA_PASSWORD: contraseña compartida por el docente
  CONSUMER_GROUP: identificador único, por ejemplo student-ana
"""

import json
import os
import sys

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
USERNAME = os.getenv("KAFKA_USERNAME", "student")
PASSWORD = os.getenv("KAFKA_PASSWORD")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "student-change-this")
TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")

missing = [
    name
    for name, value in {
        "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "KAFKA_PASSWORD": PASSWORD,
    }.items()
    if not value
]
if missing:
    raise SystemExit(f"Define estas variables de entorno: {', '.join(missing)}")

if not CONSUMER_GROUP.startswith("student-"):
    raise SystemExit("CONSUMER_GROUP debe comenzar por 'student-' (ACL del laboratorio).")

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
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
