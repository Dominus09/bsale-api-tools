"""Detección de día de entrega desde observaciones OC (pre-despacho)."""

from __future__ import annotations

import re
import unicodedata

DAY_TOKENS: tuple[str, ...] = (
    "lunes",
    "martes",
    "miercoles",
    "jueves",
    "viernes",
    "sabado",
    "domingo",
)

DAY_TOKEN_SET = frozenset(DAY_TOKENS)

DAY_LABEL: dict[str, str] = {
    "lunes": "Lunes",
    "martes": "Martes",
    "miercoles": "Miércoles",
    "jueves": "Jueves",
    "viernes": "Viernes",
    "sabado": "Sábado",
    "domingo": "Domingo",
}

_DAY_RE = re.compile(
    r"\b(" + "|".join(DAY_TOKENS) + r")\b",
    re.IGNORECASE,
)

_DELIVERY_CTX_RE = re.compile(
    r"\b(entrega|retiro|reparto|despacho)\b",
    re.IGNORECASE,
)

# Claves sin tildes → etiqueta canónica comuna
MUNICIPALITY_CANONICAL: dict[str, str] = {
    "ancud": "Ancud",
    "quellon": "Quellón",
    "castro": "Castro",
    "chonchi": "Chonchi",
    "dalcahue": "Dalcahue",
    "achao": "Achao",
    "queilen": "Queilén",
    "quemchi": "Quemchi",
    "puqueldon": "Puqueldón",
    "melinka": "Melinka",
}


def strip_accents(text: str) -> str:
    nk = unicodedata.normalize("NFD", text)
    return "".join(c for c in nk if unicodedata.category(c) != "Mn")


def normalize_day_token(raw: str | None) -> str | None:
    if raw is None or not str(raw).strip():
        return None
    s = strip_accents(str(raw).strip().lower())
    s = "".join(c for c in s if c.isalpha())
    return s if s in DAY_TOKEN_SET else None


def normalize_municipality_name(raw: str | None) -> str:
    t = (raw or "").strip()
    if not t:
        return "Sin comuna"
    key = strip_accents(t).lower()
    if key in MUNICIPALITY_CANONICAL:
        return MUNICIPALITY_CANONICAL[key]
    return t[0].upper() + t[1:] if len(t) > 1 else t.upper()


def delivery_day_label(token: str | None) -> str:
    if not token:
        return "Sin día"
    return DAY_LABEL.get(token, token.capitalize())


def detect_delivery_day_from_observation(text: str | None) -> str | None:
    """
    Extrae un único día desde texto de observación.
    Prioriza menciones cercanas a entrega/retiro; si hay varias, la última en el texto.
    """
    if text is None or not str(text).strip():
        return None
    norm = strip_accents(str(text).strip().lower())
    if not norm:
        return None

    matches = list(_DAY_RE.finditer(norm))
    if not matches:
        return None
    if len(matches) == 1:
        return normalize_day_token(matches[0].group(1))

    # Varias menciones: preferir la última con contexto de entrega; si ninguna, la última del texto.
    best: str | None = None
    best_score = -1.0
    last_token: str | None = None
    for m in matches:
        token = normalize_day_token(m.group(1))
        if not token:
            continue
        last_token = token
        start, end = m.span()
        window = norm[max(0, start - 48) : min(len(norm), end + 48)]
        score = float(start) / max(len(norm), 1)
        if _DELIVERY_CTX_RE.search(window):
            score += 10.0
        if score >= best_score:
            best_score = score
            best = token
    return best or last_token


def resolve_delivery_day(
    observaciones: str | None,
    comments: str | None = None,
    dia_atencion: str | None = None,
) -> tuple[str | None, str]:
    """
    Resuelve día de entrega desde fuentes Bsale actuales.
    1) Día en atributo OBSERVACIONES (más reciente)
    2) Día en comentarios del documento
    3) Texto combinado (obs + comentarios) — última mención con contexto
    4) Ruta / cliente (dia_atencion)
    """
    obs_text = (observaciones or "").strip()
    comments_text = (comments or "").strip()

    obs_day = detect_delivery_day_from_observation(obs_text)
    if obs_day:
        return obs_day, "observacion"

    comments_day = detect_delivery_day_from_observation(comments_text)
    if comments_day:
        return comments_day, "comentario"

    combined = "\n".join(x for x in (obs_text, comments_text) if x)
    combined_day = detect_delivery_day_from_observation(combined)
    if combined_day:
        return combined_day, "observacion"

    route = normalize_day_token(dia_atencion)
    if route:
        return route, "ruta"
    return None, "sin_dia"


def delivery_day_matches_filter(
    detected: str | None,
    filter_tokens: list[str],
) -> bool:
    if not filter_tokens:
        return True
    if not detected:
        return False
    return detected in filter_tokens


# SQL: última mención de día con límite de palabra (PostgreSQL \m \M).
_DETECT_DAY_SQL_INNER = r"""(regexp_match(
    translate(lower(COALESCE({expr}, '')), 'áéíóúü', 'aeiouu'),
    '.*\m(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\M'
))[1]"""


def sql_detect_delivery_day(expr: str) -> str:
    return _DETECT_DAY_SQL_INNER.format(expr=expr)


def sql_resolve_delivery_day(
    observaciones_expr: str,
    comments_expr: str,
    dia_atencion_expr: str,
) -> str:
    """Misma precedencia que ``resolve_delivery_day`` (Python).

    1) día en observaciones
    2) día en comentarios (aunque observaciones tenga texto sin día)
    3) día de ruta / cliente
    """
    obs_det = sql_detect_delivery_day(observaciones_expr)
    comments_det = sql_detect_delivery_day(comments_expr)
    route_det = sql_detect_delivery_day(dia_atencion_expr)
    return f"""COALESCE(
    {obs_det},
    {comments_det},
    {route_det}
)"""
