"""Plan de full reconcile de entidades (puro, sin BD): conteos, faltantes y fusible.

Política (``docs/BSALE_RAW_PHASE3_SQL_PROPOSAL.md`` §3, ``BSALE_RAW_ARCHITECTURE.md`` §4):

- En entidades NO hay borrado: los no vistos se marcan ``missing_since``.
- Sólo es faltante una fila que no está en el snapshot, del mismo company/resource/scope y con
  ``api_fetched_at <= snapshot_started_at`` (una fila refrescada durante el snapshot por otro
  mecanismo no se toca).
- Fusible antes de escribir: snapshot vacío con filas presentes, o % de filas presentes que
  pasarían a faltantes > umbral (20 % por defecto) → la corrida falla sin escribir nada.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Hashable, Iterable

DEFAULT_MAX_MISSING_PCT = 20.0


def max_missing_pct(resource: str, getenv: Callable[[str], str | None] = os.getenv) -> float:
    """``BSALE_RAW_MAX_MISSING_PCT_<RESOURCE>`` > ``BSALE_RAW_MAX_MISSING_PCT`` > 20."""
    for name in (f"BSALE_RAW_MAX_MISSING_PCT_{resource.upper()}", "BSALE_RAW_MAX_MISSING_PCT"):
        raw = (getenv(name) or "").strip()
        if raw:
            value = float(raw)
            if not 0 <= value <= 100:
                raise ValueError(f"{name} debe estar entre 0 y 100")
            return value
    return DEFAULT_MAX_MISSING_PCT


@dataclass(frozen=True)
class ExistingRow:
    bsale_id: int
    payload_hash: str
    api_fetched_at: datetime
    missing_since: datetime | None


@dataclass
class ReconcilePlan:
    snapshot_started_at: datetime
    threshold_pct: float
    present_existing: int
    already_missing: int
    missing_ids: list[Any]
    protected_newer: int
    missing_pct: float
    fuse_tripped: bool
    fuse_reason: str | None
    predicted: dict[str, int] = field(default_factory=dict)
    _classes: dict[Any, str] = field(default_factory=dict, repr=False)

    def fuse_json(self) -> dict[str, Any]:
        return {
            "threshold_pct": self.threshold_pct,
            "present_existing": self.present_existing,
            "already_missing": self.already_missing,
            "would_mark_missing": len(self.missing_ids),
            "protected_newer": self.protected_newer,
            "missing_pct": self.missing_pct,
            "tripped": self.fuse_tripped,
            "reason": self.fuse_reason,
        }

    def counts(self, applied_ids: Iterable[Any]) -> dict[str, int]:
        """Conteos reales a partir de las filas que el UPSERT efectivamente aplicó."""
        applied = set(applied_ids)
        out = {"inserted": 0, "updated": 0, "unchanged": 0, "skipped_newer": 0}
        for bsale_id, kind in self._classes.items():
            if bsale_id not in applied:
                out["skipped_newer"] += 1
            elif kind == "skipped_newer":
                # La fila destino cambió entre la lectura y el UPSERT: se aplicó igual.
                out["updated"] += 1
            else:
                out[kind] += 1
        return out


def entity_key(row: Any) -> Hashable:
    return row.bsale_id


def plan_reconcile(
    existing: dict[Hashable, ExistingRow],
    rows: list[Any],
    *,
    snapshot_started_at: datetime,
    threshold_pct: float,
    key: Callable[[Any], Hashable] = entity_key,
) -> ReconcilePlan:
    """``existing`` y ``key(row)`` usan la misma clave: ``bsale_id`` en entidades, ``(variant_id, office_id)`` en stock."""
    classes: dict[Hashable, str] = {}
    for row in rows:
        row_key = key(row)
        prev = existing.get(row_key)
        if prev is None:
            classes[row_key] = "inserted"
        elif prev.api_fetched_at > row.api_fetched_at:
            classes[row_key] = "skipped_newer"
        elif prev.payload_hash == row.payload_hash:
            classes[row_key] = "unchanged"
        else:
            classes[row_key] = "updated"

    seen = set(classes)
    present = [(k, e) for k, e in existing.items() if e.missing_since is None]
    absent = [(k, e) for k, e in present if k not in seen]
    missing_ids = sorted(k for k, e in absent if e.api_fetched_at <= snapshot_started_at)
    protected_newer = len(absent) - len(missing_ids)
    missing_pct = round(len(missing_ids) * 100.0 / len(present), 2) if present else 0.0

    reason = None
    if not rows and present:
        reason = f"snapshot vacío con {len(present)} filas presentes"
    elif missing_pct > threshold_pct:
        reason = (
            f"{len(missing_ids)}/{len(present)} filas pasarían a faltantes "
            f"({missing_pct}%) > umbral {threshold_pct}%"
        )

    predicted = {"inserted": 0, "updated": 0, "unchanged": 0, "skipped_newer": 0}
    for kind in classes.values():
        predicted[kind] += 1

    return ReconcilePlan(
        snapshot_started_at=snapshot_started_at,
        threshold_pct=threshold_pct,
        present_existing=len(present),
        already_missing=len(existing) - len(present),
        missing_ids=missing_ids,
        protected_newer=protected_newer,
        missing_pct=missing_pct,
        fuse_tripped=reason is not None,
        fuse_reason=reason,
        predicted=predicted,
        _classes=classes,
    )
