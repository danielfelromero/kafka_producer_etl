#!/usr/bin/env bash
set -Eeuo pipefail

# Alias de compatibilidad para quienes utilizaban el comando anterior.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
echo "Este comando se conserva por compatibilidad; ahora utiliza Bore (sin ngrok)."
exec "$SCRIPT_DIR/start_classroom.sh" "$@"
