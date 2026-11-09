#!/usr/bin/env bash
set -Eeuo pipefail

# Arranque completo del perfil CLASSROOM usando Bore (bore.pub).
# No requiere cuenta, tarjeta, token ni un plan de pago.
# El endpoint TCP se asigna dinámicamente.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$PROJECT_ROOT"

STATE_DIR="${PROJECT_ROOT}/.kafka"
TOOLS_DIR="${PROJECT_ROOT}/.tools"
ENV_FILE="${PROJECT_ROOT}/.env.classroom"
mkdir -p "$STATE_DIR" "$TOOLS_DIR"
chmod 700 "$STATE_DIR" "$TOOLS_DIR"

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

for command_name in curl tar sha256sum python; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Falta el comando requerido: $command_name" >&2
    exit 2
  fi
done

BORE_VERSION="0.6.0"
BORE_BIN="${TOOLS_DIR}/bore"
BORE_LOG="${STATE_DIR}/bore.log"
BORE_PID_FILE="${STATE_DIR}/tunnel.pid"
ENDPOINT_FILE="${STATE_DIR}/tunnel_address"
KAFKA_PID_FILE="${STATE_DIR}/kafka-server.pid"
BORE_PID=""

if [[ -s "$BORE_PID_FILE" ]] && kill -0 "$(cat "$BORE_PID_FILE")" 2>/dev/null; then
  echo "Ya existe un proceso Bore activo (PID $(cat "$BORE_PID_FILE"))." >&2
  exit 2
fi

if [[ -s "$KAFKA_PID_FILE" ]] && kill -0 "$(cat "$KAFKA_PID_FILE")" 2>/dev/null; then
  echo "Kafka ya parece estar activo (PID $(cat "$KAFKA_PID_FILE"))." >&2
  echo "Deténlo primero con ./stop_kafka.sh." >&2
  exit 2
fi

# Descarga una versión fijada de Bore y verifica el SHA-256 del release oficial.
if [[ ! -x "$BORE_BIN" ]]; then
  case "$(uname -m)" in
    x86_64)
      BORE_ARCH="x86_64-unknown-linux-musl"
      BORE_SHA256="e484d1e3acba77169b773f31a5bfb34192d4b660f44a094a658a2522cd2270f7"
      ;;
    aarch64|arm64)
      BORE_ARCH="aarch64-unknown-linux-musl"
      BORE_SHA256="ffc4515f3617420b243758cf36ed6a63208d7dba76b2ec3e90d1f476a9742951"
      ;;
    *)
      echo "Arquitectura no soportada por el instalador automático de Bore: $(uname -m)" >&2
      echo "Consulta los binarios oficiales: https://github.com/ekzhang/bore/releases/tag/v${BORE_VERSION}" >&2
      exit 2
      ;;
  esac

  ARCHIVE="${TOOLS_DIR}/bore-v${BORE_VERSION}-${BORE_ARCH}.tar.gz"
  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "$TMP_DIR"' EXIT

  DOWNLOAD_URL="https://github.com/ekzhang/bore/releases/download/v${BORE_VERSION}/bore-v${BORE_VERSION}-${BORE_ARCH}.tar.gz"
  echo "Descargando Bore v${BORE_VERSION}..."
  curl -fL --retry 3 --retry-delay 2 "$DOWNLOAD_URL" -o "$ARCHIVE"
  echo "${BORE_SHA256}  ${ARCHIVE}" | sha256sum -c -
  tar -xzf "$ARCHIVE" -C "$TMP_DIR"

  if [[ ! -f "${TMP_DIR}/bore" ]]; then
    echo "El archivo del release no contiene el ejecutable esperado." >&2
    exit 1
  fi
  install -m 755 "${TMP_DIR}/bore" "$BORE_BIN"
  rm -rf "$TMP_DIR"
  trap - EXIT
fi

rm -f "$BORE_PID_FILE" "$ENDPOINT_FILE"

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  echo
  echo "Deteniendo el entorno de aula..."

  if [[ -f "$KAFKA_PID_FILE" ]]; then
    ./stop_kafka.sh || true
  fi

  if [[ -n "$BORE_PID" ]] && kill -0 "$BORE_PID" 2>/dev/null; then
    kill "$BORE_PID" 2>/dev/null || true
    wait "$BORE_PID" 2>/dev/null || true
  fi

  rm -f "$BORE_PID_FILE" "$ENDPOINT_FILE"
  echo "Entorno de aula detenido."
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "1/3 Abriendo túnel TCP con Bore hacia 127.0.0.1:19092..."
"$BORE_BIN" local 19092 --local-host 127.0.0.1 --to bore.pub >"$BORE_LOG" 2>&1 &
BORE_PID=$!
echo "$BORE_PID" > "$BORE_PID_FILE"

echo "2/3 Esperando la dirección TCP asignada por bore.pub..."
if ! python - "$ENDPOINT_FILE" "$BORE_PID" "$BORE_LOG" <<'PY'
import os
import re
import sys
import time
from pathlib import Path

destination = Path(sys.argv[1])
bore_pid = int(sys.argv[2])
log_path = Path(sys.argv[3])
deadline = time.time() + 60
pattern = re.compile(r"listening at bore\.pub:(\d+)")

while time.time() < deadline:
    try:
        content = log_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        content = ""

    match = pattern.search(content)
    if match:
        address = f"bore.pub:{match.group(1)}"
        destination.write_text(address + "\n", encoding="utf-8")
        print(f"Endpoint TCP asignado: {address}")
        sys.exit(0)

    try:
        os.kill(bore_pid, 0)
    except OSError:
        break

    time.sleep(0.5)

print("No fue posible obtener el endpoint TCP de bore.pub.", file=sys.stderr)
if log_path.exists():
    print(log_path.read_text(encoding="utf-8", errors="replace")[-6000:], file=sys.stderr)
sys.exit(1)
PY
then
  echo "Error iniciando Bore. Log disponible en .kafka/bore.log:" >&2
  tail -n 60 "$BORE_LOG" >&2 || true
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
echo "Seguridad Kafka: PLAINTEXT sin autenticación ni cifrado."
echo "Bore transporta el tráfico mediante un túnel TCP."
echo "Cualquier persona que conozca el endpoint puede intentar conectarse."
echo "Mantén esta terminal abierta durante la sesión."
echo "Presiona Ctrl+C para detener Kafka y Bore."
echo "============================================================"
echo

wait "$BORE_PID"
