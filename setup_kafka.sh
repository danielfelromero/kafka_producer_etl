#!/usr/bin/env bash
set -Eeuo pipefail

# Bootstrap de Kafka KRaft de un solo broker para kafka_producer_etl.
# Sigue el enfoque comprobado en etl-kafka-blocks-lab.

KAFKA_VERSION="3.9.1"
KAFKA_DIR="kafka_2.13-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
KAFKA_HOME="${PROJECT_ROOT}/${KAFKA_DIR}"
KAFKA_CONFIG="${KAFKA_HOME}/config/kraft/server.properties"
STATE_DIR="${PROJECT_ROOT}/.kafka"
KAFKA_LOG="${KAFKA_LOG:-${STATE_DIR}/kafka-server.log}"
KAFKA_PID="${KAFKA_PID:-${STATE_DIR}/kafka-server.pid}"
KAFKA_DATA="${KAFKA_DATA:-${STATE_DIR}/data}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-etl.records}"

mkdir -p "$STATE_DIR" "$KAFKA_DATA"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
  if [[ ! -f "$KAFKA_ARCHIVE" ]]; then
    echo "Descargando Apache Kafka ${KAFKA_VERSION}..."
    curl -fL --retry 3 --retry-delay 2 "$KAFKA_URL" -o "$KAFKA_ARCHIVE"
  fi
  tar -xzf "$KAFKA_ARCHIVE"
fi

if [[ -f "$KAFKA_PID" ]] && kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" >/dev/null 2>&1; then
    echo "Kafka ya está activo en $BOOTSTRAP_SERVERS (PID $(cat "$KAFKA_PID"))."
    exit 0
  fi
  echo "Hay un PID registrado, pero Kafka no responde. Revisa $KAFKA_LOG." >&2
  tail -n 60 "$KAFKA_LOG" || true
  exit 1
fi
rm -f "$KAFKA_PID"

if ! grep -q '^log.dirs=' "$KAFKA_CONFIG"; then
  printf '\nlog.dirs=%s\n' "$KAFKA_DATA" >> "$KAFKA_CONFIG"
else
  sed -i.bak "s#^log.dirs=.*#log.dirs=$KAFKA_DATA#" "$KAFKA_CONFIG"
fi

if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "Inicializando almacenamiento KRaft para un broker..."
  CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
fi

echo "Iniciando Kafka..."
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" >"$KAFKA_LOG" 2>&1 &
echo $! > "$KAFKA_PID"

ready=false
for attempt in $(seq 1 90); do
  if ! kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
    echo "Kafka terminó durante el inicio. Últimas líneas del log:" >&2
    tail -n 80 "$KAFKA_LOG" >&2 || true
    rm -f "$KAFKA_PID"
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "$ready" != true ]]; then
  echo "Kafka no estuvo listo en 180 segundos. Últimas líneas del log:" >&2
  tail -n 80 "$KAFKA_LOG" >&2 || true
  exit 1
fi

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1

echo "Kafka está listo en $BOOTSTRAP_SERVERS."
echo "Topic: $TOPIC (3 particiones, replication-factor 1)."
echo "CLI disponible en: $KAFKA_HOME/bin/"
echo "Verifica con: ./diagnose_kafka.sh"
echo "Detén Kafka con: ./stop_kafka.sh"
echo "Log: $KAFKA_LOG"
