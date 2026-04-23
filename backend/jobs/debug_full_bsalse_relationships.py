"""Compat: nombre con typo; usar ``backend.jobs.debug_full_bsale_relationships``."""

from backend.jobs.debug_full_bsale_relationships import main

if __name__ == "__main__":
    raise SystemExit(main())
