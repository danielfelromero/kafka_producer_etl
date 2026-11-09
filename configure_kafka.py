"""Generate a Kafka KRaft broker configuration for LOCAL or CLASSROOM mode."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


OVERRIDE_KEYS = {
    "controller.quorum.voters",
    "controller.listener.names",
    "log.dirs",
    "listeners",
    "advertised.listeners",
    "inter.broker.listener.name",
    "listener.security.protocol.map",
    "sasl.enabled.mechanisms",
    "listener.name.external.plain.sasl.jaas.config",
    "authorizer.class.name",
    "super.users",
    "allow.everyone.if.no.acl.found",
}

def generate_config(
    template_path: Path,
    destination_path: Path,
    data_dir: Path,
    profile: str,
    external_address: str = "",
) -> str:
    """Render the generated config while preserving unrelated KRaft settings."""
    normalized_profile = profile.strip().upper()
    if normalized_profile not in {"LOCAL", "CLASSROOM"}:
        raise ValueError("profile must be LOCAL or CLASSROOM")

    if normalized_profile == "CLASSROOM":
        if not re.fullmatch(r"bore\.pub:[0-9]{1,5}", external_address):
            raise ValueError("CLASSROOM requires a valid bore.pub:port address")
        port = int(external_address.rsplit(":", 1)[1])
        if not 1 <= port <= 65535:
            raise ValueError("external port must be between 1 and 65535")

    lines = template_path.read_text(encoding="utf-8").splitlines()
    kept: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            kept.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key not in OVERRIDE_KEYS:
            kept.append(line)

    overrides = {
        "controller.quorum.voters": "1@127.0.0.1:9093",
        "controller.listener.names": "CONTROLLER",
        "log.dirs": str(data_dir.resolve()),
    }

    if normalized_profile == "LOCAL":
        overrides.update(
            {
                "listeners": (
                    "PLAINTEXT://127.0.0.1:9092,"
                    "CONTROLLER://127.0.0.1:9093"
                ),
                "advertised.listeners": "PLAINTEXT://localhost:9092",
                "inter.broker.listener.name": "PLAINTEXT",
                "listener.security.protocol.map": (
                    "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                ),
            }
        )
    else:
        overrides.update(
            {
                "listeners": (
                    "INTERNAL://127.0.0.1:9092,"
                    "EXTERNAL://127.0.0.1:19092,"
                    "CONTROLLER://127.0.0.1:9093"
                ),
                "advertised.listeners": (
                    "INTERNAL://localhost:9092,"
                    f"EXTERNAL://{external_address}"
                ),
                "inter.broker.listener.name": "INTERNAL",
                "listener.security.protocol.map": (
                    "CONTROLLER:PLAINTEXT,"
                    "INTERNAL:PLAINTEXT,"
                    "EXTERNAL:PLAINTEXT"
                ),
            }
        )

    kept.extend(f"{key}={value}" for key, value in overrides.items())
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text("\n".join(kept) + "\n", encoding="utf-8")
    destination_path.chmod(0o600)
    return destination_path.read_text(encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--profile", required=True, choices=["LOCAL", "CLASSROOM"])
    parser.add_argument("--external-address", default="")
    args = parser.parse_args()

    generate_config(
        template_path=args.template,
        destination_path=args.destination,
        data_dir=args.data_dir,
        profile=args.profile,
        external_address=args.external_address,
    )
    print(f"Configuración generada: {args.destination}")
    print(f"Perfil: {args.profile}")
    if args.profile == "CLASSROOM":
        print("Listener interno: localhost:9092")
        print("Listener externo Kafka: 127.0.0.1:19092")
        print(f"Dirección externa anunciada: {args.external_address}")
        print("Seguridad externa: PLAINTEXT sin autenticación ni cifrado.")


if __name__ == "__main__":
    main()
