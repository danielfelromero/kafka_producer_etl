#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

ENV_FILE="${PROJECT_ROOT}/.env.classroom"
if [[ -f "$ENV_FILE" ]]; then
  # This file is a local configuration file maintained by the repository owner.
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${NGROK_DOMAIN:?Define NGROK_DOMAIN in .env.classroom (without tls:// or port).}"

if [[ ! -x "$(command -v ngrok 2>/dev/null || true)" ]]; then
  echo "No se encuentra ngrok. Instálalo siguiendo la sección 'Publicar el broker con ngrok' del README." >&2
  exit 2
fi

if [[ ! -f "${PROJECT_ROOT}/.kafka/profile" ]] || \
   [[ "$(cat "${PROJECT_ROOT}/.kafka/profile")" != "CLASSROOM" ]]; then
  echo "Kafka no está configurado con KAFKA_PROFILE=CLASSROOM." >&2
  echo "Ejecuta: source .env.classroom && ./setup_kafka.sh" >&2
  exit 2
fi

if ! (echo > /dev/tcp/127.0.0.1/19092) >/dev/null 2>&1; then
  echo "No se detecta el listener Kafka externo en 127.0.0.1:19092." >&2
  echo "Comprueba .kafka/kafka-server.log y la configuración del broker." >&2
  exit 2
fi

if [[ ! -f "${PROJECT_ROOT}/ngrok-classroom-policy.yml" ]]; then
  echo "No se encuentra ngrok-classroom-policy.yml." >&2
  exit 2
fi

echo "Publicando listener Kafka 127.0.0.1:19092 como TLS endpoint:"
echo "  tls://${NGROK_DOMAIN}"
echo "El endpoint termina TLS en ngrok y reenvía el tráfico al listener SASL de Kafka."
echo "Mantén esta terminal abierta durante la sesión."
exec ngrok tls 19092 \
  --url "tls://${NGROK_DOMAIN}" \
  --traffic-policy-file "${PROJECT_ROOT}/ngrok-classroom-policy.yml"
