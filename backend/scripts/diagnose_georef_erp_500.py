"""
Diagnóstico GET /operaciones/georef-pendientes?vista=erp&solo_pendientes=false

Uso (con venv y variables PG_* en el entorno):
  python -m backend.scripts.diagnose_georef_erp_500
"""
from __future__ import annotations

import traceback

REQUIRED_RUTERO_COLS = (
    "georef_estado",
    "lat_operacional",
    "lon_operacional",
    "motivo_rechazo",
    "fecha_rechazo",
    "usuario_rechazo",
)


def main() -> None:
    from backend.db import get_connection
    from backend.services import rutero_georef_service as svc

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'bsale'
          AND table_name = 'rutero'
          AND column_name = ANY(%s)
        ORDER BY 1
        """,
        (list(REQUIRED_RUTERO_COLS),),
    )
    found = {r[0] for r in cur.fetchall()}
    missing = set(REQUIRED_RUTERO_COLS) - found
    print("Columnas bsale.rutero encontradas:", sorted(found))
    print("Columnas FALTANTES:", sorted(missing) or "(ninguna)")
    cur.close()
    conn.close()

    print("\n--- get_georef_resumen ---")
    try:
        print(svc.get_georef_resumen(None))
    except Exception as e:
        print(type(e).__name__, e)
        traceback.print_exc()

    print("\n--- list_georef_erp solo_pendientes=False ---")
    try:
        items = svc.list_georef_erp(None, solo_pendientes=False)
        print("OK, filas:", len(items))
    except Exception as e:
        print(type(e).__name__, e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
