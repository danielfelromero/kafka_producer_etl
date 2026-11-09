#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
KAFKA_VERSION="3.9.1"
KAFKA_HOME="${PROJECT_ROOT}/kafka_2.13-${KAFKA_VERSION}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-etl.records}"
PROFILE_FILE="${PROJECT_ROOT}/.kafka/profile"
CONFIG_FILE="${PROJECT_ROOT}/.kafka/server.properties"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" ]]; then
  echo "No se encuentra Kafka CLI. Ejecuta primero ./setup_kafka.sh" >&2
  exit 2
fi

echo "Comprobando broker interno en ${BOOTSTRAP_SERVERS}..."
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

if [[ -f "$PROFILE_FILE" && "$(cat "$PROFILE_FILE")" == "CLASSROOM" ]]; then
  echo
  echo "Perfil: CLASSROOM"
  echo "Listeners y metadata anunciada:"
  grep -E '^(listeners|advertised.listeners|inter.broker.listener.name|listener.security.protocol.map|authorizer.class.name|super.users)=' "$CONFIG_FILE" || true
  echo
  echo "ACL del topic:"
  "${KAFKA_HOME}/bin/kafka-acls.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --list --topic "$TOPIC"
  echo
  echo "ACL de consumer groups student- (prefijo):"
  "${KAFKA_HOME}/bin/kafka-acls.sh" --bootstrap-server "${BOOTSTRAP_SERVERS}" --list --group "student-" --resource-pattern-type prefixed
  echo
  echo "Contraseña del consumer almacenada en .kafka/student_password."
fi
