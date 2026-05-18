"""
Reglas operacionales de visitas (panel operaciones).

Alineado a ``chk_visitas_estado`` en PostgreSQL. Estados futuros se pueden añadir aquí
sin cambiar contratos de la app móvil.
"""

from __future__ import annotations

# Cierre operacional válido: cuenta como visita realizada.
ESTADOS_VISITA_REALIZADA: tuple[str, ...] = (
    "visitado",
    "incidencia",
    # Reservados por si se amplía el CHECK en BD (hoy no existen en prod):
    "sin_compra",
    "cerrado",
    "gestionado",
)

# Solo los que existen hoy en ``bsale.visitas`` (sql/bsale_rutas_visitas.sql).
ESTADOS_VISITA_REALIZADA_VIGENTES: tuple[str, ...] = ("visitado", "incidencia")

ESTADO_PENDIENTE = "pendiente"
ESTADO_INCIDENCIA = "incidencia"


def es_visita_realizada(estado: str | None) -> bool:
    if not estado:
        return False
    e = str(estado).strip().lower()
    return e in ESTADOS_VISITA_REALIZADA_VIGENTES or e in {
        x for x in ESTADOS_VISITA_REALIZADA if x not in ESTADOS_VISITA_REALIZADA_VIGENTES
    }


def sql_in_estados_realizados() -> str:
    """Fragmento SQL: ``estado IN ('visitado', 'incidencia')``."""
    return ", ".join(f"'{s}'" for s in ESTADOS_VISITA_REALIZADA_VIGENTES)
