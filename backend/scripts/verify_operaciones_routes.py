#!/usr/bin/env python3
"""Lista rutas POST de telemetría (ejecutar: python -m backend.scripts.verify_operaciones_routes)."""

from __future__ import annotations

from backend.main import app

KEYS = ("heartbeat", "gps")


def main() -> None:
    print("Rutas telemetría registradas:\n")
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", None) or set()
        if not any(k in path for k in KEYS):
            continue
        if "POST" not in methods:
            continue
        print(f"  POST {path}")
    print("\nOpenAPI: GET /docs → tag 'Operaciones Telemetría Móvil'")
    print("Health:  GET /operaciones/telemetry/health")


if __name__ == "__main__":
    main()
