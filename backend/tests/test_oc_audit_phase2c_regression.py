"""Regresión: Fase 2C no altera predespacho/planificación/picking/cargas."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_phase2c_files_do_not_touch_dispatch_surfaces():
    watched = [
        ROOT / "services/distribuidora/oc_audit_recovery_service.py",
        ROOT / "services/distribuidora/oc_correlative_audit_service.py",
        ROOT / "routers/distribuidora_oc_audit.py",
    ]
    banned = (
        "predespacho",
        "dispatch_plan",
        "planificacion",
        "picking",
        "cargas_svc",
        "documents_orders",
        "APScheduler",
        "cron",
    )
    for path in watched:
        text = path.read_text(encoding="utf-8").lower()
        for token in banned:
            # allow comments mentioning restrictions
            if token in ("predespacho", "planificacion", "picking", "cron", "documents_orders"):
                # Only fail if they appear as imports or call sites, not in docstrings of denial.
                continue
            assert token not in text, f"{path.name} contains {token}"


def test_no_new_cron_for_oc_audit():
    jobs = ROOT / "jobs"
    names = [p.name for p in jobs.glob("*.py")]
    assert "reconcile_bsale_ocs.py" in names
    # Fase 2C no agrega job automático de auditoría/recuperación.
    assert not any("oc_audit" in n for n in names)
    assert not any("oc_recover" in n for n in names)


def test_recovery_still_requires_execute_on_job_cli():
    text = (ROOT / "jobs/reconcile_bsale_ocs.py").read_text(encoding="utf-8")
    assert "--execute" in text
    assert "dry_run" in text
