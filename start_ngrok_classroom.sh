#!/usr/bin/env bash
set -Eeuo pipefail

# Arranque completo del perfil CLASSROOM:
# 1. Abre un endpoint TCP gratuito de ngrok.
# 2. Captura el host:puerto asignado dinámicamente.
# 3. Arranca Kafka anunciando ese endpoint.
#
# Sin TLS, SASL, autenticación ni cifrado de aplicación.
# Uso exclusivo para laboratorio temporal con datos no sensibles.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

STATE_DIR="${PROJECT_ROOT}/.kafka"
ENV_FILE="${PROJECT_ROOT}/.env.classroom"
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "No existe .env.classroom." >&2
  echo "Créalo con: cp .env.classroom.example .env.classroom" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

if [[ "${KAFKA_PROFILE:-}" != "CLASSROOM" ]]; then
  echo "Configura KAFKA_PROFILE=CLASSROOM en .env.classroom." >&2
  exit 2
fi

if ! command -v ngrok >/dev/null 2>&1; then
  echo "No se encuentra ngrok. Instálalo siguiendo el README." >&2
  exit 2
fi

NGROK_PID_FILE="${STATE_DIR}/ngrok.pid"
ENDPOINT_FILE="${STATE_DIR}/ngrok_tcp_address"
NGROK_LOG="${STATE_DIR}/ngrok.log"
KAFKA_PID_FILE="${STATE_DIR}/kafka-server.pid"
NGROK_PID=""

if [[ -s "$NGROK_PID_FILE" ]] && kill -0 "$(cat "$NGROK_PID_FILE")" 2>/dev/null; then
  echo "Ya existe un proceso ngrok activo (PID $(cat "$NGROK_PID_FILE"))." >&2
  exit 2
fi

if [[ -s "$KAFKA_PID_FILE" ]] && kill -0 "$(cat "$KAFKA_PID_FILE")" 2>/dev/null; then
  echo "Kafka ya parece estar activo (PID $(cat "$KAFKA_PID_FILE"))." >&2
  echo "Deténlo primero con ./stop_kafka.sh y vuelve a ejecutar este script." >&2
  exit 2
fi

rm -f "$NGROK_PID_FILE" "$ENDPOINT_FILE"

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  echo
  echo "Deteniendo el entorno de aula..."

  if [[ -f "$KAFKA_PID_FILE" ]]; then
    ./stop_kafka.sh || true
  fi

  if [[ -n "$NGROK_PID" ]] && kill -0 "$NGROK_PID" 2>/dev/null; then
    kill "$NGROK_PID" 2>/dev/null || true
    wait "$NGROK_PID" 2>/dev/null || true
  fi

  rm -f "$NGROK_PID_FILE" "$ENDPOINT_FILE"
  echo "Entorno de aula detenido."
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "1/3 Iniciando ngrok TCP hacia 127.0.0.1:19092..."
ngrok tcp 19092 --log=stdout >"$NGROK_LOG" 2>&1 &
NGROK_PID=$!
echo "$NGROK_PID" > "$NGROK_PID_FILE"

echo "2/3 Esperando la dirección TCP asignada por ngrok..."
if ! python - "$ENDPOINT_FILE" "$NGROK_PID" <<'PY'
import json
import os
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

destination = Path(sys.argv[1])
ngrok_pid = int(sys.argv[2])
deadline = time.time() + 60

while time.time() < deadline:
    try:
        with urlopen("http://127.0.0.1:4040/api/tunnels", timeout=2) as response:
            payload = json.load(response)

        for tunnel in payload.get("tunnels", []):
            public_url = tunnel.get("public_url", "")
            if not public_url.startswith("tcp://"):
                continue
            parsed = urlsplit(public_url)
            if not parsed.hostname or not parsed.port:
                continue
            address = f"{parsed.hostname}:{parsed.port}"
            destination.write_text(address + "\n", encoding="utf-8")
            print(f"Endpoint TCP: {address}")
            sys.exit(0)
    except (URLError, TimeoutError, json.JSONDecodeError):
        pass

    try:
        os.kill(ngrok_pid, 0)
    except OSError:
        break

    time.sleep(1)

print("No fue posible obtener el endpoint TCP de ngrok.", file=sys.stderr)
sys.exit(1)
PY
then
  echo "Error iniciando ngrok. Log disponible en .kafka/ngrok.log:" >&2
  tail -n 60 "$NGROK_LOG" >&2 || true
  exit 1
fi

echo "3/3 Iniciando Kafka en modo CLASSROOM..."
./setup_kafka.sh

echo
echo "============================================================"
echo "ENTORNO DE AULA LISTO"
echo "Kafka local del docente: localhost:9092"
echo "Endpoint TCP para estudiantes: $(cat "$ENDPOINT_FILE")"
echo "Topic: ${KAFKA_TOPIC:-etl.records}"
echo "Seguridad: PLAINTEXT sin autenticación ni cifrado."
echo "Comparte el endpoint con los estudiantes."
echo "Mantén esta terminal abierta durante la sesión."
echo "Presiona Ctrl+C para detener Kafka y ngrok."
echo "============================================================"
echo

wait "$NGROK_PID"
