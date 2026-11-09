#!/usr/bin/env bash
set -Eeuo pipefail

# Abre un endpoint TCP gratuito de ngrok (requiere método de pago verificado
# por ngrok, pero no activa un plan de pago ni se cobra por la verificación).
# El endpoint se asigna dinámicamente y se guarda para advertised.listeners.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

STATE_DIR="${PROJECT_ROOT}/.kafka"
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

ENV_FILE="${PROJECT_ROOT}/.env.classroom"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

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

if [[ -s "$NGROK_PID_FILE" ]] && kill -0 "$(cat "$NGROK_PID_FILE")" 2>/dev/null; then
  echo "Ya existe un proceso ngrok activo (PID $(cat "$NGROK_PID_FILE"))." >&2
  echo "Deténlo antes de iniciar otro." >&2
  exit 2
fi

rm -f "$NGROK_PID_FILE" "$ENDPOINT_FILE"

echo "Iniciando ngrok TCP hacia 127.0.0.1:19092..."
echo "El endpoint público será dinámico (host:puerto)."
ngrok tcp 19092 --log=stdout >"$NGROK_LOG" 2>&1 &
NGROK_PID=$!
echo "$NGROK_PID" > "$NGROK_PID_FILE"

cleanup() {
  if kill -0 "$NGROK_PID" 2>/dev/null; then
    kill "$NGROK_PID" 2>/dev/null || true
    wait "$NGROK_PID" 2>/dev/null || true
  fi
  rm -f "$NGROK_PID_FILE" "$ENDPOINT_FILE"
}
trap cleanup EXIT INT TERM

# Consultar la API local del agente ngrok para capturar la dirección TCP
# que fue asignada dinámicamente. Kafka necesita conocerla antes de iniciar
# para anunciarla correctamente en advertised.listeners.
if ! python - "$ENDPOINT_FILE" "$NGROK_PID" <<'PY'
import json
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from urllib.parse import urlsplit

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
            print(f"Endpoint TCP asignado por ngrok: {address}")
            print("Ahora abre otra terminal y ejecuta ./setup_kafka.sh.")
            sys.exit(0)
    except (URLError, TimeoutError, json.JSONDecodeError):
        pass

    try:
        import os
        os.kill(ngrok_pid, 0)
    except OSError:
        break

    time.sleep(1)

print("No fue posible obtener el endpoint TCP desde la API local de ngrok.", file=sys.stderr)
print("Revisa el log: .kafka/ngrok.log", file=sys.stderr)
sys.exit(1)
PY
then
  tail -n 60 "$NGROK_LOG" >&2 || true
  exit 1
fi

echo
echo "Túnel activo. No cierres esta terminal durante la sesión."
echo "Cuando termines, detén Kafka primero y luego presiona Ctrl+C aquí."
wait "$NGROK_PID"
