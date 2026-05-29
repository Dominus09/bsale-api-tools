"""Día operativo en bsale.rutero (alineado a app móvil y mapa)."""

from __future__ import annotations

from datetime import date

# Tabla aliased como r
SQL_DIA_OPERATIVO_R = """(
    CASE
        WHEN LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) = 'sabado' THEN 'Sabado'
        ELSE TRIM(COALESCE(r.dia_atencion::text, ''))
    END
)"""

_DIA_SEMANA_ES = (
    "lunes",
    "martes",
    "miércoles",
    "jueves",
    "viernes",
    "sábado",
    "domingo",
)


def dia_atencion_desde_fecha(fecha: date) -> str:
    return _DIA_SEMANA_ES[fecha.weekday()]


def where_dia_operativo_r(dia: str) -> tuple[str, list]:
    """Comparación sin tildes, igual que GET /vendedor/ruta."""
    return (
        f"""translate(lower(trim({SQL_DIA_OPERATIVO_R})), 'áéíóúü', 'aeiouu')
            = translate(lower(trim(%s)), 'áéíóúü', 'aeiouu')""",
        [dia],
    )
