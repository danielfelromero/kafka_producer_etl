#!/usr/bin/env bash
set -Eeuo pipefail

# Kafka 3.9.1 en KRaft: perfil LOCAL o CLASSROOM.
# CLASSROOM utiliza un endpoint TCP público de Bore sin TLS ni autenticación.
# Úsalo solo para laboratorio temporal y con datos no sensibles.

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

EXTERNAL_ADDRESS=""
if [[ "$PROFILE" == "CLASSROOM" ]]; then
  ENDPOINT_FILE="${STATE_DIR}/tunnel_address"
  TUNNEL_PID_FILE="${STATE_DIR}/tunnel.pid"

  if [[ ! -s "$ENDPOINT_FILE" ]]; then
    echo "No se encuentra la dirección TCP del túnel." >&2
    echo "Ejecuta primero ./start_classroom.sh y deja esa terminal abierta." >&2
    exit 2
  fi
  if [[ ! -s "$TUNNEL_PID_FILE" ]] || ! kill -0 "$(cat "$TUNNEL_PID_FILE")" 2>/dev/null; then
    echo "El proceso del túnel no está activo. Inicia de nuevo ./start_classroom.sh." >&2
    exit 2
  fi

  EXTERNAL_ADDRESS="$(tr -d '[:space:]' < "$ENDPOINT_FILE")"
  if [[ ! "$EXTERNAL_ADDRESS" =~ ^bore\.pub:[0-9]+$ ]]; then
    echo "Dirección TCP inválida en $ENDPOINT_FILE: $EXTERNAL_ADDRESS" >&2
    exit 2
  fi
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

# Renderiza la configuración con una función Python testeable.
python "${PROJECT_ROOT}/configure_kafka.py" \
  --template "$KAFKA_TEMPLATE" \
  --destination "$KAFKA_CONFIG" \
  --data-dir "$KAFKA_DATA" \
  --profile "$PROFILE" \
  --external-address "$EXTERNAL_ADDRESS"

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

printf '%s\n' "$PROFILE" > "$PROFILE_FILE"
chmod 600 "$PROFILE_FILE"

echo
echo "Kafka está listo."
echo "Perfil: $PROFILE"
echo "Producer local: localhost:9092"
echo "Topic: $TOPIC (3 particiones, replication-factor 1)"
if [[ "$PROFILE" == "CLASSROOM" ]]; then
  echo "Consumer externo: $EXTERNAL_ADDRESS"
  echo "Protocolo: PLAINTEXT (sin autenticación ni cifrado)."
  echo "Cualquier persona que conozca el endpoint puede acceder al broker."
fi
echo "Verifica con: ./diagnose_kafka.sh"
echo "Detén Kafka con: ./stop_kafka.sh"
echo "Log: $KAFKA_LOG"
