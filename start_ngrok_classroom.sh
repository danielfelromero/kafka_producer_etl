#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

ENV_FILE="${PROJECT_ROOT}/.env.classroom"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${NGROK_DOMAIN:?Define NGROK_DOMAIN in .env.classroom (without tls:// or port).}"

if ! command -v ngrok >/dev/null 2>&1; then
  echo "No se encuentra ngrok. Instálalo siguiendo el README." >&2
  exit 2
fi

if [[ ! -f "${PROJECT_ROOT}/.kafka/profile" ]] || \
   [[ "$(cat "${PROJECT_ROOT}/.kafka/profile")" != "CLASSROOM" ]]; then
  echo "Kafka no está configurado con KAFKA_PROFILE=CLASSROOM." >&2
  echo "Carga .env.classroom y ejecuta ./setup_kafka.sh antes de iniciar ngrok." >&2
  exit 2
fi

if [[ ! -f "${PROJECT_ROOT}/.kafka/server.properties" ]] || \
   ! grep -Fq "EXTERNAL://${NGROK_DOMAIN}:443" "${PROJECT_ROOT}/.kafka/server.properties"; then
  echo "El dominio anunciado por Kafka no coincide con NGROK_DOMAIN." >&2
  echo "Detén Kafka y vuelve a ejecutar ./setup_kafka.sh después de actualizar .env.classroom." >&2
  exit 2
fi

if ! (echo > /dev/tcp/127.0.0.1/19092) >/dev/null 2>&1; then
  echo "No se detecta el listener Kafka externo en 127.0.0.1:19092." >&2
  echo "Comprueba .kafka/kafka-server.log y la configuración del broker." >&2
  exit 2
fi

POLICY_FILE="${PROJECT_ROOT}/ngrok-classroom-policy.yml"
if [[ ! -f "$POLICY_FILE" ]]; then
  echo "No se encuentra ngrok-classroom-policy.yml." >&2
  exit 2
fi

echo "Publicando Kafka como TLS endpoint:"
echo "  tls://${NGROK_DOMAIN}:443 -> 127.0.0.1:19092"
echo "TLS termina en el edge de ngrok; Kafka exige SASL/PLAIN en el listener externo."
echo "Mantén esta terminal abierta durante la sesión."
exec ngrok tls 19092 \
  --url "tls://${NGROK_DOMAIN}" \
  --traffic-policy-file "$POLICY_FILE"
