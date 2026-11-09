#!/usr/bin/env bash
set -Eeuo pipefail

# Kafka 3.9.1 en KRaft: modo LOCAL o CLASSROOM.
# LOCAL: productor y clientes locales en localhost:9092.
# CLASSROOM: listener interno + listener SASL_PLAINTEXT detrás de un
# endpoint TLS ngrok que termina TLS en su edge.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

KAFKA_VERSION="3.9.1"
KAFKA_DIR="kafka_2.13-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
KAFKA_HOME="${PROJECT_ROOT}/${KAFKA_DIR}"
KAFKA_TEMPLATE="${KAFKA_HOME}/config/kraft/server.properties"
STATE_DIR="${PROJECT_ROOT}/.kafka"
KAFKA_CONFIG="${STATE_DIR}/server.properties"
KAFKA_LOG="${KAFKA_LOG:-${STATE_DIR}/kafka-server.log}"
KAFKA_PID="${KAFKA_PID:-${STATE_DIR}/kafka-server.pid}"
KAFKA_DATA="${KAFKA_DATA:-${STATE_DIR}/data}"
PROFILE_FILE="${STATE_DIR}/profile"
PROFILE="${KAFKA_PROFILE:-LOCAL}"
TOPIC="${KAFKA_TOPIC:-etl.records}"

PROFILE="$(printf '%s' "$PROFILE" | tr '[:lower:]' '[:upper:]')"
if [[ "$PROFILE" != "LOCAL" && "$PROFILE" != "CLASSROOM" ]]; then
  echo "KAFKA_PROFILE debe ser LOCAL o CLASSROOM." >&2
  exit 2
fi

if [[ "$PROFILE" == "CLASSROOM" ]]; then
  : "${NGROK_DOMAIN:?Define NGROK_DOMAIN con el dominio TLS asignado por ngrok, sin esquema ni puerto.}"
  if [[ ! "$NGROK_DOMAIN" =~ ^[A-Za-z0-9.-]+$ ]]; then
    echo "NGROK_DOMAIN no parece un hostname válido: $NGROK_DOMAIN" >&2
    echo "Usa únicamente el host, por ejemplo mi-dominio.ngrok.app." >&2
    exit 2
  fi
  command -v openssl >/dev/null 2>&1 || {
    echo "Se requiere openssl para generar la contraseña del consumer." >&2
    exit 2
  }
fi

mkdir -p "$STATE_DIR" "$KAFKA_DATA"
chmod 700 "$STATE_DIR"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
  if [[ ! -f "$KAFKA_ARCHIVE" ]]; then
    echo "Descargando Apache Kafka ${KAFKA_VERSION}..."
    curl -fL --retry 3 --retry-delay 2 "$KAFKA_URL" -o "$KAFKA_ARCHIVE"
  fi
  tar -xzf "$KAFKA_ARCHIVE"
fi

if [[ -f "$KAFKA_PID" ]] && kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
  CURRENT_PROFILE="$(cat "$PROFILE_FILE" 2>/dev/null || echo UNKNOWN)"
  if [[ "$CURRENT_PROFILE" == "$PROFILE" ]] && \
     "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka ya está activo en localhost:9092 (perfil $PROFILE, PID $(cat "$KAFKA_PID"))."
    exit 0
  fi
  echo "Kafka parece estar activo con perfil '$CURRENT_PROFILE'." >&2
  echo "Ejecuta ./stop_kafka.sh antes de cambiar el perfil o la configuración." >&2
  exit 1
fi
rm -f "$KAFKA_PID"

STUDENT_PASSWORD=""
if [[ "$PROFILE" == "CLASSROOM" ]]; then
  PASSWORD_FILE="${STATE_DIR}/student_password"
  if [[ ! -s "$PASSWORD_FILE" ]]; then
    openssl rand -hex 24 > "$PASSWORD_FILE"
  fi
  chmod 600 "$PASSWORD_FILE"
  STUDENT_PASSWORD="$(cat "$PASSWORD_FILE")"
fi

# Genera una configuración independiente; no modifica el archivo de ejemplo
# incluido en la distribución de Apache Kafka.
export KAFKA_TEMPLATE KAFKA_CONFIG KAFKA_DATA PROFILE TOPIC
export NGROK_DOMAIN="${NGROK_DOMAIN:-}"
export STUDENT_PASSWORD
python - <<'PY'
import os
from pathlib import Path

template = Path(os.environ["KAFKA_TEMPLATE"])
destination = Path(os.environ["KAFKA_CONFIG"])
profile = os.environ["PROFILE"]
data_dir = str(Path(os.environ["KAFKA_DATA"]).resolve())
topic = os.environ["TOPIC"]

lines = template.read_text(encoding="utf-8").splitlines()
override_keys = {
    "controller.quorum.voters",
    "controller.listener.names",
    "log.dirs",
    "listeners",
    "advertised.listeners",
    "inter.broker.listener.name",
    "listener.security.protocol.map",
    "sasl.enabled.mechanisms",
    "listener.name.external.plain.sasl.jaas.config",
    "authorizer.class.name",
    "super.users",
    "allow.everyone.if.no.acl.found",
}
kept = [
    line for line in lines
    if not any(line.strip().startswith(f"{key}=") for key in override_keys)
]

overrides = {
    "controller.quorum.voters": "1@127.0.0.1:9093",
    "controller.listener.names": "CONTROLLER",
    "log.dirs": data_dir,
}

if profile == "LOCAL":
    overrides.update({
        "listeners": "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
        "advertised.listeners": "PLAINTEXT://localhost:9092",
        "inter.broker.listener.name": "PLAINTEXT",
        "listener.security.protocol.map": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    })
else:
    domain = os.environ["NGROK_DOMAIN"]
    password = os.environ["STUDENT_PASSWORD"]
    overrides.update({
        "listeners": (
            "INTERNAL://127.0.0.1:9092,"
            "EXTERNAL://127.0.0.1:19092,"
            "CONTROLLER://127.0.0.1:9093"
        ),
        "advertised.listeners": (
            f"INTERNAL://localhost:9092,EXTERNAL://{domain}:443"
        ),
        "inter.broker.listener.name": "INTERNAL",
        "listener.security.protocol.map": (
            "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT"
        ),
        "sasl.enabled.mechanisms": "PLAIN",
        "listener.name.external.plain.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'user_student="{password}";'
        ),
        "authorizer.class.name": (
            "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        ),
        # Anonymous is privileged only on the loopback-only INTERNAL listener,
        # which is used by the instructor's local producer and admin commands.
        "super.users": "User:ANONYMOUS",
        "allow.everyone.if.no.acl.found": "false",
    })

kept.extend(f"{key}={value}" for key, value in overrides.items())
destination.write_text("\n".join(kept) + "\n", encoding="utf-8")
destination.chmod(0o600)
print(f"Configuración generada: {destination}")
print(f"Perfil: {profile}")
if profile == "CLASSROOM":
    print("Listener interno: localhost:9092")
    print(f"Listener externo anunciado: {domain}:443")
    print("Listener Kafka para el túnel: 127.0.0.1:19092 (SASL/PLAIN)")
    print(f"Topic: {topic}")
PY

if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "Inicializando almacenamiento KRaft para un broker..."
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
fi

echo "Iniciando Kafka con perfil $PROFILE..."
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" >"$KAFKA_LOG" 2>&1 &
echo $! > "$KAFKA_PID"

ready=false
for attempt in $(seq 1 90); do
  if ! kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
    echo "Kafka terminó durante el inicio. Últimas líneas del log:" >&2
    tail -n 100 "$KAFKA_LOG" >&2 || true
    rm -f "$KAFKA_PID"
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "$ready" != true ]]; then
  echo "Kafka no estuvo listo en 180 segundos. Últimas líneas del log:" >&2
  tail -n 100 "$KAFKA_LOG" >&2 || true
  exit 1
fi

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1

if [[ "$PROFILE" == "CLASSROOM" ]]; then
  echo "Configurando ACL de solo lectura para User:student..."
  "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server localhost:9092 \
    --add --allow-principal User:student \
    --operation Read --operation Describe --topic "$TOPIC"

  "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server localhost:9092 \
    --add --allow-principal User:student \
    --operation Read --operation Describe \
    --group "student-" --resource-pattern-type prefixed

  printf '%s\n' "$PROFILE" > "$PROFILE_FILE"
  chmod 600 "$PROFILE_FILE"

  echo
  echo "Kafka está listo para CLASSROOM."
  echo "Producer/admin local: localhost:9092."
  echo "Listener Kafka para ngrok: 127.0.0.1:19092."
  echo "Endpoint externo que anunciará Kafka: $NGROK_DOMAIN:443."
  echo "Topic: $TOPIC (3 particiones, replication-factor 1)."
  echo "Usuario de lectura: student."
  echo "Contraseña local: $PASSWORD_FILE (no se versiona)."
  echo "Ahora inicia ngrok con: ./start_ngrok_classroom.sh"
else
  printf '%s\n' "$PROFILE" > "$PROFILE_FILE"
  chmod 600 "$PROFILE_FILE"
  echo
  echo "Kafka está listo en localhost:9092."
  echo "Topic: $TOPIC (3 particiones, replication-factor 1)."
  echo "Perfil: LOCAL."
fi

echo "CLI disponible en: $KAFKA_HOME/bin/"
echo "Verifica con: ./diagnose_kafka.sh"
echo "Detén Kafka con: ./stop_kafka.sh"
echo "Log: $KAFKA_LOG"
