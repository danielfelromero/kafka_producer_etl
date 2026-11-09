#!/usr/bin/env bash
set -Eeuo pipefail

# Alias histórico de compatibilidad. El transporte actual utiliza Bore.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
echo "Comando conservado por compatibilidad: utiliza Bore (no ngrok)."
exec "$SCRIPT_DIR/start_classroom.sh" "$@"
