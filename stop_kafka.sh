#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
KAFKA_PID="${KAFKA_PID:-${PROJECT_ROOT}/.kafka/kafka-server.pid}"

if [[ ! -f "$KAFKA_PID" ]]; then
  echo "No hay archivo PID en $KAFKA_PID; Kafka puede estar detenido."
  exit 0
fi

pid="$(cat "$KAFKA_PID")"
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "Kafka no se detuvo de forma controlada. Revisa .kafka/kafka-server.log." >&2
    exit 1
  fi
fi

rm -f "$KAFKA_PID"
echo "Kafka detenido. Los datos se conservan en .kafka/data."
