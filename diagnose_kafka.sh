#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
KAFKA_VERSION="3.9.1"
KAFKA_HOME="${PROJECT_ROOT}/kafka_2.13-${KAFKA_VERSION}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-etl.records}"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" ]]; then
  echo "No se encuentra Kafka CLI. Ejecuta primero ./setup_kafka.sh" >&2
  exit 2
fi

echo "Comprobando broker en ${BOOTSTRAP_SERVERS}..."
"${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}"

echo
echo "Topics disponibles:"
"${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --list

echo
echo "Descripción de ${TOPIC}:"
"${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${TOPIC}"

echo
echo "Offsets por partición:"
"${KAFKA_HOME}/bin/kafka-get-offsets.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --topic "${TOPIC}"
