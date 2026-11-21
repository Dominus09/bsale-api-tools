"""Asserts de aislamiento Offline V2 staging.

Aborta si detecta hosts/DSN/buckets productivos.
No imprime secretos.
"""

from __future__ import annotations

import os
import re
import sys


BANNED_HOST_SUBSTR = (
    "api.quillotana.cl",
    "app.quillotana.cl",
    "work.quillotana.cl",
    "test.quillotana.cl",
    "quillotana.cl",
    "analytics",
    "neon.tech",
    "supabase.co",
    "rds.amazonaws.com",
)

BANNED_DSN_SUBSTR = (
    "quillotana",
    "analytics",
    "prod",
    "production",
)

BANNED_BUCKET_SUBSTR = (
    "prod",
    "production",
    "quillotana-operaciones",  # bucket prod típico
)


def _fail(msg: str) -> None:
    print(f"[ISOLATION-FAIL] {msg}", flush=True)
    raise SystemExit(2)


def _check_value(label: str, value: str, banned: tuple[str, ...]) -> None:
    low = (value or "").strip().lower()
    if not low:
        return
    for b in banned:
        if b in low:
            _fail(f"{label} contiene patrón prohibido {b!r}")


def assert_staging_isolation(*, require_writes_off: bool = True) -> dict:
    dsn = (
        os.getenv("STAGING_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SIMPLE_PLANNING_TEST_DSN")
        or ""
    ).strip()
    api_url = (os.getenv("STAGING_API_PUBLIC_URL") or os.getenv("NEXT_PUBLIC_API_URL") or "").strip()
    proxy = (os.getenv("API_PROXY_TARGET") or "").strip()
    r2_bucket = (os.getenv("R2_BUCKET") or "").strip()
    r2_endpoint = (os.getenv("R2_ENDPOINT") or os.getenv("R2_ACCOUNT_ID") or "").strip()
    jwt = (os.getenv("JWT_SECRET_KEY") or os.getenv("SECRET_KEY") or "").strip()
    profile = (os.getenv("OFFLINE_V2_ENV_PROFILE") or "").strip().lower()

    if profile and profile not in {"staging", "lab", "local-lab"}:
        _fail(f"OFFLINE_V2_ENV_PROFILE={profile!r} no es staging/lab")

    if not dsn:
        _fail("Falta STAGING_DATABASE_URL / DATABASE_URL / SIMPLE_PLANNING_TEST_DSN")

    _check_value("DSN", dsn, BANNED_DSN_SUBSTR + BANNED_HOST_SUBSTR)

    for label, val in (
        ("STAGING_API_PUBLIC_URL/NEXT_PUBLIC_API_URL", api_url),
        ("API_PROXY_TARGET", proxy),
    ):
        _check_value(label, val, BANNED_HOST_SUBSTR)

    _check_value("R2_BUCKET", r2_bucket, BANNED_BUCKET_SUBSTR)
    if r2_bucket and re.search(r"operaciones", r2_bucket, re.I) and "lab" not in r2_bucket.lower():
        _fail("R2_BUCKET parece productivo (operaciones sin 'lab')")

    if jwt and jwt.lower() in {"change-me", "secret", "production"}:
        _fail("JWT_SECRET_KEY débil o placeholder")

    writes = (os.getenv("OFFLINE_SYNC_WRITES_ENABLED") or "").strip().lower()
    if require_writes_off and writes in {"1", "true", "yes", "on"}:
        # Permitido solo si kill-switch no está y allowlist definida — arranque exige OFF.
        if (os.getenv("OFFLINE_V2_ALLOW_WRITES_AT_BOOT") or "").strip() != "1":
            _fail(
                "OFFLINE_SYNC_WRITES_ENABLED está ON al boot; "
                "arranque staging exige OFF (kill switch)."
            )

    report = {
        "ok": True,
        "dsn_host_local": ("127.0.0.1" in dsn or "localhost" in dsn),
        "writes_enabled": writes in {"1", "true", "yes", "on"},
        "r2_bucket_set": bool(r2_bucket),
        "api_public_set": bool(api_url),
        "lab_route_flag": os.getenv("OFFLINE_V2_LAB_ENABLED") == "1",
    }
    print(f"[ISOLATION-OK] {report}", flush=True)
    return report


def main() -> int:
    require_off = "--allow-writes-on" not in sys.argv
    assert_staging_isolation(require_writes_off=require_off)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
