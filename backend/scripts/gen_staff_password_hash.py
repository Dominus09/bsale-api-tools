"""Print bcrypt hash for bsale.users.password_hash (passlib, same as auth router)."""
import argparse
import sys

from passlib.context import CryptContext

ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def main() -> None:
    p = argparse.ArgumentParser(description="Generate bcrypt hash for staff login")
    p.add_argument("password", help="Plain password (avoid shell history: use -f or env)")
    p.add_argument("--email", help="If set, print a suggested SQL UPDATE line")
    args = p.parse_args()
    h = ctx.hash(args.password)
    print(h)
    if args.email:
        esc = args.email.replace("'", "''")
        print(
            f"UPDATE bsale.users SET password_hash = '{h}' "
            f"WHERE lower(email) = lower('{esc}') AND active = true;",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
