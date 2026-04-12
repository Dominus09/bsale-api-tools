"""
Genera un hash bcrypt listo para pegar en SQL o usar en login (bsale.vendedores_app).

Uso (desde la raíz del repo, con dependencias instaladas):
  python -m backend.scripts.gen_vendedores_app_password_hash "TuPasswordSegura"

Login en backend (psycopg2, sin ORM): tras leer password_hash de BD:
  import bcrypt
  ok = bcrypt.checkpw(plain.encode("utf-8"), stored_hash.encode("utf-8"))

No guardar la contraseña en texto plano ni en logs.
"""

from __future__ import annotations

import argparse
import sys

import bcrypt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hash bcrypt para vendedores de la app móvil (bsale.vendedores_app)"
    )
    parser.add_argument(
        "password",
        nargs="?",
        default=None,
        help="Contraseña en texto plano (omitir y usar --default para Laquillotana123)",
    )
    parser.add_argument(
        "--default",
        action="store_true",
        help='Usar la contraseña inicial documentada: "Laquillotana123"',
    )
    args = parser.parse_args()

    if args.default:
        plain = "Laquillotana123"
    elif args.password:
        plain = args.password
    else:
        parser.print_help()
        sys.exit(1)

    raw = plain.encode("utf-8")
    # rounds=12 equilibra coste y seguridad (ajustable)
    hashed = bcrypt.hashpw(raw, bcrypt.gensalt(rounds=12))
    print(hashed.decode("ascii"), flush=True)


if __name__ == "__main__":
    main()
