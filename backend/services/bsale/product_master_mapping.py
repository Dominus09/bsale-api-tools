"""
Identidad autoritativa producto canónico ERP (``products_master.id``) ↔ variante Bsale por empresa
(``bsale.product_master_variants``, esquema en ``backend/sql/051_product_master_variants_sync_columns.sql``).

Reglas:
- Identidad Bsale = (company_id, variant_id). Un variant_id nunca se cruza entre empresas.
- 1 variante con el barcode en la empresa → AUTO_EXACT; 0 → MISSING; >1 → AMBIGUOUS (sin elegir).
- Filas MANUAL nunca se modifican.
- Una variante ya reclamada por otra ficha (MANUAL o fuera del alcance del cálculo), reclamada por
  varias fichas a la vez, o sin product_id (el CHECK exige product_id en AUTO_EXACT) → AMBIGUOUS.
- mapping_source: AUTO_EXACT/AMBIGUOUS → 'BARCODE'; MISSING conserva la procedencia previa
  (p. ej. 'LEGACY_COMPANIES'); filas MANUAL no se tocan.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

from psycopg2.extras import execute_batch, execute_values

AUTO_EXACT = "AUTO_EXACT"
MISSING = "MISSING"
AMBIGUOUS = "AMBIGUOUS"
MANUAL = "MANUAL"

SOURCE_BARCODE = "BARCODE"


@dataclass(frozen=True)
class VariantRef:
    company_id: int
    variant_id: int
    product_id: int | None
    barcode: str


@dataclass(frozen=True)
class ExistingMapping:
    product_master_id: int
    company_id: int
    mapping_status: str | None
    variant_id: int | None
    product_id: int | None
    match_count: int | None
    candidate_variant_ids: tuple[int, ...] | None
    barcode: str | None
    mapping_source: str | None = SOURCE_BARCODE


@dataclass(frozen=True)
class MappingDecision:
    product_master_id: int
    company_id: int
    barcode: str
    mapping_status: str
    mapping_source: str
    product_id: int | None
    variant_id: int | None
    match_count: int
    candidate_variant_ids: tuple[int, ...]
    changed: bool
    previous_variant_id: int | None


def normalize_barcode(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _as_tuple(value: Any) -> tuple[int, ...]:
    if not value:
        return ()
    return tuple(int(x) for x in value)


def _mapping_source_for(status: str, prev: ExistingMapping | None) -> str:
    """
    AUTO_EXACT/AMBIGUOUS se deciden por variantes actuales + barcode → BARCODE.
    MISSING conserva la procedencia previa (LEGACY_COMPANIES o BARCODE) para auditoría.
    """
    if status == MISSING and prev is not None and prev.mapping_source:
        return prev.mapping_source
    return SOURCE_BARCODE


def compute_mappings(
    masters: Iterable[tuple[int, str]],
    variants: Iterable[VariantRef],
    existing: Iterable[ExistingMapping],
) -> tuple[list[MappingDecision], int]:
    """
    Devuelve (decisiones, filas_manual_omitidas).

    Pares evaluados = filas ya existentes ∪ (ficha, empresa) con variantes que comparten el barcode.
    No depende de ``products_master.companies``.
    """
    master_barcode: dict[int, str] = {}
    for pm_id, bc in masters:
        nb = normalize_barcode(bc)
        if nb:
            master_barcode[int(pm_id)] = nb

    by_company_barcode: dict[tuple[int, str], list[VariantRef]] = defaultdict(list)
    companies_by_barcode: dict[str, set[int]] = defaultdict(set)
    for v in variants:
        nb = normalize_barcode(v.barcode)
        if not nb:
            continue
        by_company_barcode[(v.company_id, nb)].append(v)
        companies_by_barcode[nb].add(v.company_id)
    for lst in by_company_barcode.values():
        lst.sort(key=lambda x: x.variant_id)

    existing_map: dict[tuple[int, int], ExistingMapping] = {}
    external_claims: dict[tuple[int, int], int] = {}
    for e in existing:
        existing_map[(e.product_master_id, e.company_id)] = e
        if e.variant_id is not None and (
            e.mapping_status == MANUAL or e.product_master_id not in master_barcode
        ):
            external_claims[(e.company_id, int(e.variant_id))] = e.product_master_id

    pairs: set[tuple[int, int]] = {k for k in existing_map if k[0] in master_barcode}
    for pm_id, nb in master_barcode.items():
        for cid in companies_by_barcode.get(nb, ()):
            pairs.add((pm_id, cid))

    auto_claims: dict[tuple[int, int], set[int]] = defaultdict(set)
    candidates_by_pair: dict[tuple[int, int], list[VariantRef]] = {}
    manual_skipped = 0
    for pm_id, cid in pairs:
        prev = existing_map.get((pm_id, cid))
        if prev is not None and prev.mapping_status == MANUAL:
            manual_skipped += 1
            continue
        cands = by_company_barcode.get((cid, master_barcode[pm_id]), [])
        candidates_by_pair[(pm_id, cid)] = cands
        if len(cands) == 1:
            auto_claims[(cid, cands[0].variant_id)].add(pm_id)

    decisions: list[MappingDecision] = []
    for (pm_id, cid), cands in sorted(candidates_by_pair.items()):
        n = len(cands)
        status: str = MISSING
        product_id: int | None = None
        variant_id: int | None = None
        cand_ids: tuple[int, ...] = ()
        if n == 1:
            v = cands[0]
            claim = external_claims.get((cid, v.variant_id))
            conflict = (
                (claim is not None and claim != pm_id)
                or len(auto_claims[(cid, v.variant_id)]) > 1
                or v.product_id is None
            )
            if conflict:
                status, cand_ids = AMBIGUOUS, (v.variant_id,)
            else:
                status, product_id, variant_id = AUTO_EXACT, v.product_id, v.variant_id
        elif n > 1:
            status, cand_ids = AMBIGUOUS, tuple(c.variant_id for c in cands)

        barcode = master_barcode[pm_id]
        prev = existing_map.get((pm_id, cid))
        source = _mapping_source_for(status, prev)
        changed = prev is None or (
            prev.mapping_status,
            prev.mapping_source,
            prev.variant_id,
            prev.product_id,
            prev.match_count,
            _as_tuple(prev.candidate_variant_ids),
            prev.barcode,
        ) != (status, source, variant_id, product_id, n, cand_ids, barcode)
        decisions.append(
            MappingDecision(
                product_master_id=pm_id,
                company_id=cid,
                barcode=barcode,
                mapping_status=status,
                mapping_source=source,
                product_id=product_id,
                variant_id=variant_id,
                match_count=n,
                candidate_variant_ids=cand_ids,
                changed=changed,
                previous_variant_id=prev.variant_id if prev is not None else None,
            )
        )
    return decisions, manual_skipped


_EXISTING_COLUMNS = """
    product_master_id, company_id, mapping_status, variant_id, product_id,
    match_count, candidate_variant_ids, barcode, mapping_source
"""


def row_to_existing(row: tuple) -> ExistingMapping:
    return ExistingMapping(
        product_master_id=int(row[0]),
        company_id=int(row[1]),
        mapping_status=row[2],
        variant_id=int(row[3]) if row[3] is not None else None,
        product_id=int(row[4]) if row[4] is not None else None,
        match_count=int(row[5]) if row[5] is not None else None,
        candidate_variant_ids=_as_tuple(row[6]),
        barcode=row[7],
        mapping_source=row[8],
    )


_RELEASE_SQL = """
UPDATE bsale.product_master_variants
SET mapping_status = 'MISSING',
    product_id = NULL,
    variant_id = NULL
WHERE product_master_id = %s AND company_id = %s
  AND mapping_status IS DISTINCT FROM 'MANUAL'
"""

_UPSERT_SQL = """
INSERT INTO bsale.product_master_variants AS pmv (
    product_master_id, company_id, barcode, mapping_status, mapping_source,
    product_id, variant_id, match_count, candidate_variant_ids, last_verified_at, updated_at
) VALUES %s
ON CONFLICT (product_master_id, company_id) DO UPDATE SET
    barcode = EXCLUDED.barcode,
    mapping_status = EXCLUDED.mapping_status,
    mapping_source = EXCLUDED.mapping_source,
    product_id = EXCLUDED.product_id,
    variant_id = EXCLUDED.variant_id,
    match_count = EXCLUDED.match_count,
    candidate_variant_ids = EXCLUDED.candidate_variant_ids,
    last_verified_at = NOW(),
    updated_at = CASE
        WHEN (pmv.barcode, pmv.mapping_status, pmv.mapping_source, pmv.product_id,
              pmv.variant_id, pmv.match_count, pmv.candidate_variant_ids)
             IS DISTINCT FROM
             (EXCLUDED.barcode, EXCLUDED.mapping_status, EXCLUDED.mapping_source,
              EXCLUDED.product_id, EXCLUDED.variant_id, EXCLUDED.match_count,
              EXCLUDED.candidate_variant_ids)
        THEN NOW()
        ELSE pmv.updated_at
    END
WHERE pmv.mapping_status IS DISTINCT FROM 'MANUAL'
"""

_UPSERT_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s::bigint[],NOW(),NOW())"


def decision_to_row(d: MappingDecision) -> tuple:
    if d.mapping_status == AUTO_EXACT:
        if d.variant_id is None or d.product_id is None:
            raise ValueError(f"AUTO_EXACT sin variant/product (pm={d.product_master_id} company={d.company_id})")
    elif d.mapping_status in (MISSING, AMBIGUOUS):
        if d.variant_id is not None:
            raise ValueError(f"{d.mapping_status} con variant_id (pm={d.product_master_id} company={d.company_id})")
    else:
        raise ValueError(f"mapping_status no escribible por el sync: {d.mapping_status}")
    return (
        d.product_master_id,
        d.company_id,
        d.barcode,
        d.mapping_status,
        d.mapping_source,
        d.product_id,
        d.variant_id,
        d.match_count,
        list(d.candidate_variant_ids),
    )


def persist_mappings(cur: Any, decisions: list[MappingDecision]) -> int:
    """
    Escribe decisiones (sin COMMIT). Nunca toca filas MANUAL.

    1) Filas no MANUAL cuyo variant_id cambia pasan transitoriamente a MISSING sin variante
       (cumple el CHECK de mapping y evita choques con UNIQUE (company_id, variant_id)).
    2) UPSERT por (product_master_id, company_id); ``updated_at`` sólo cambia si cambió algo.
    """
    if not decisions:
        return 0
    rows = [decision_to_row(d) for d in decisions]
    release = [
        (d.product_master_id, d.company_id)
        for d in decisions
        if d.previous_variant_id is not None and d.previous_variant_id != d.variant_id
    ]
    if release:
        execute_batch(cur, _RELEASE_SQL, release, page_size=500)
    execute_values(cur, _UPSERT_SQL, rows, template=_UPSERT_TEMPLATE, page_size=1000)
    return len(rows)


def summarize(decisions: list[MappingDecision], manual_skipped: int) -> dict[str, int]:
    out = {AUTO_EXACT: 0, MISSING: 0, AMBIGUOUS: 0}
    for d in decisions:
        out[d.mapping_status] += 1
    return {
        "auto_exact": out[AUTO_EXACT],
        "missing": out[MISSING],
        "ambiguous": out[AMBIGUOUS],
        "manual_skipped": manual_skipped,
        "changed": sum(1 for d in decisions if d.changed),
    }


def refresh_all_mappings(cur: Any) -> dict[str, int]:
    """Recalcula todos los pares (sin COMMIT)."""
    cur.execute("SELECT id, barcode FROM bsale.products_master")
    masters = [(int(r[0]), r[1]) for r in cur.fetchall()]
    cur.execute(
        """
        SELECT company_id, bsale_id, product_id, bar_code
        FROM bsale.variants
        WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL
        """
    )
    variants = [
        VariantRef(int(r[0]), int(r[1]), int(r[2]) if r[2] is not None else None, r[3])
        for r in cur.fetchall()
    ]
    cur.execute(f"SELECT {_EXISTING_COLUMNS} FROM bsale.product_master_variants")
    existing = [row_to_existing(r) for r in cur.fetchall()]

    decisions, manual_skipped = compute_mappings(masters, variants, existing)
    persist_mappings(cur, decisions)
    return summarize(decisions, manual_skipped)


def upsert_mapping_for_pair(
    cur: Any, *, product_master_id: int, company_id: int, barcode: str
) -> dict[str, Any]:
    """
    Recalcula un único par (ficha, empresa) — usado al crear ficha desde una variante (sin COMMIT).
    """
    nb = normalize_barcode(barcode)
    cur.execute(
        """
        SELECT company_id, bsale_id, product_id, bar_code
        FROM bsale.variants
        WHERE company_id = %s AND BTRIM(bar_code) = %s
        """,
        (company_id, nb),
    )
    variants = [
        VariantRef(int(r[0]), int(r[1]), int(r[2]) if r[2] is not None else None, r[3])
        for r in cur.fetchall()
    ]
    variant_ids = [v.variant_id for v in variants]
    cur.execute(
        f"""
        SELECT {_EXISTING_COLUMNS}
        FROM bsale.product_master_variants
        WHERE (product_master_id = %s AND company_id = %s)
           OR (company_id = %s AND variant_id = ANY(%s::bigint[]))
        """,
        (product_master_id, company_id, company_id, variant_ids),
    )
    existing = [row_to_existing(r) for r in cur.fetchall()]

    current = next(
        (e for e in existing if e.product_master_id == product_master_id and e.company_id == company_id),
        None,
    )
    if current is not None and current.mapping_status == MANUAL:
        return {"mapping_status": MANUAL, "variant_id": current.variant_id, "changed": False}

    decisions, _ = compute_mappings([(product_master_id, nb)], variants, existing)
    mine = [d for d in decisions if d.product_master_id == product_master_id and d.company_id == company_id]
    persist_mappings(cur, mine)
    if not mine:
        return {"mapping_status": None, "variant_id": None, "changed": False}
    d = mine[0]
    return {
        "mapping_status": d.mapping_status,
        "variant_id": d.variant_id,
        "candidate_variant_ids": list(d.candidate_variant_ids),
        "changed": d.changed,
    }
