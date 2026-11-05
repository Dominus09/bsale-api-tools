"""Cantidad de peonetas del despacho (assistant_count + peonetas_recorded)."""

from __future__ import annotations

from typing import Any


# Límite alineado al CHECK existente en 021_dispatch_plan.sql (≤10).
PEONETAS_MAX = 10


class PeonetasValidationError(ValueError):
    def __init__(self, message: str, *, code: str = "peonetas_invalid"):
        super().__init__(message)
        self.code = code


def parse_peonetas_count(value: Any) -> int:
    """Entero no negativo (0..PEONETAS_MAX). Rechaza None, bool, decimales y texto."""
    if value is None:
        raise PeonetasValidationError(
            "Indique la cantidad de peonetas (0 si no hay)",
            code="peonetas_required",
        )
    if isinstance(value, bool):
        raise PeonetasValidationError("Cant. de peonetas inválida")
    if isinstance(value, float):
        if not value.is_integer():
            raise PeonetasValidationError("Cant. de peonetas debe ser un entero")
        value = int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s or not s.isdigit():
            raise PeonetasValidationError("Cant. de peonetas debe ser un entero")
        value = int(s)
    if not isinstance(value, int):
        raise PeonetasValidationError("Cant. de peonetas debe ser un entero")
    if value < 0:
        raise PeonetasValidationError("Cant. de peonetas no puede ser negativa")
    if value > PEONETAS_MAX:
        raise PeonetasValidationError(
            f"Cant. de peonetas no puede superar {PEONETAS_MAX} "
            "(límite del esquema dispatch_plan.assistant_count)",
        )
    return value


def format_peonetas_display(
    *,
    assistant_count: Any = None,
    peonetas_recorded: Any = None,
) -> str:
    """Etiqueta UI: número informado, o 'No informado' (nunca asumir 0 histórico)."""
    try:
        count = int(assistant_count) if assistant_count is not None else 0
    except (TypeError, ValueError):
        count = 0
    if isinstance(peonetas_recorded, str):
        recorded = peonetas_recorded.strip().lower() in ("1", "true", "t", "yes")
    else:
        recorded = peonetas_recorded is True

    if recorded:
        return str(max(0, count))
    if count > 0:
        # ORS / planes previos con crew explícito > 0.
        return str(count)
    return "No informado"


def dispatch_plan_has_peonetas_recorded_column(cur) -> bool:
    """Tolerante a esquemas sin migración 054."""
    try:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'distribuidora'
                  AND table_name = 'dispatch_plan'
                  AND column_name = 'peonetas_recorded'
            )
            """
        )
        row = cur.fetchone()
        if not row:
            return False
        return bool(row[0])
    except Exception:
        return False


def mark_peonetas_recorded(cur, *, plan_id: int, assistant_count: int) -> bool:
    """Marca peonetas informadas si la columna existe. Retorna True si escribió."""
    count = parse_peonetas_count(assistant_count)
    if not dispatch_plan_has_peonetas_recorded_column(cur):
        # Sin 054: assistant_count ya quedó en el INSERT; no hay bandera.
        return False
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan
        SET assistant_count = %s,
            peonetas_recorded = TRUE,
            updated_at = NOW()
        WHERE id = %s
        """,
        (count, int(plan_id)),
    )
    return True
