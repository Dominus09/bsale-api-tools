from fastapi import APIRouter, BackgroundTasks, HTTPException

from backend.db import get_connection

router = APIRouter(prefix="/erp", tags=["ERP"])

# DASHBOARD
@router.get("/dashboard")
def get_dashboard():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_dashboard
    """)

    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]

    result = dict(zip(columns, row))

    cur.close()
    conn.close()

    return result


# ALERTAS
@router.get("/alerts")
def get_alerts():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_alerts
        LIMIT 200
    """)

    columns = [desc[0] for desc in cur.description]

    rows = cur.fetchall()

    data = [dict(zip(columns,row)) for row in rows]

    cur.close()
    conn.close()

    return data


# MARGENES
@router.get("/margins")
def get_margins():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_margin_dashboard
        LIMIT 500
    """)

    columns = [desc[0] for desc in cur.description]

    rows = cur.fetchall()

    data = [dict(zip(columns,row)) for row in rows]

    cur.close()
    conn.close()

    return data


@router.post("/sync-distribuidora")
def post_sync_distribuidora():
    """
    Sincronización incremental Bsale → distribuidora.documents / document_details
    (company_id=3, office_id=1). Requiere BSALE_TOKEN o BSALE_TOKEN_SPA en el entorno.
    """
    from backend.jobs.sync_bsale_distribuidora import sync_bsale_distribuidora

    try:
        return sync_bsale_distribuidora(strict_token=True)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/resync-distribuidora")
def post_resync_distribuidora(background_tasks: BackgroundTasks):
    """
    Encola la re-sincronización histórica de ``distribuidora.documents`` en segundo plano
    (evita timeout 524 en Cloudflare). El trabajo usa el mismo advisory lock que el sync
    incremental. Requiere BSALE_TOKEN o BSALE_TOKEN_SPA.
    """
    from backend.jobs.sync_bsale_distribuidora import (
        bsale_token_distribuidora_configured,
        run_resync_distribuidora_background,
    )

    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el resync.",
        )

    background_tasks.add_task(run_resync_distribuidora_background)
    return {"status": "resync iniciado"}


@router.post("/sync-oc-invoice-status")
def post_sync_oc_invoice_status(background_tasks: BackgroundTasks):
    """
    Actualiza ``is_invoiced`` en ``distribuidora.documents`` para OCs (tipo 33) vía
    Bsale ``/documents/{id}/references.json``. Tarea en segundo plano; requiere token.
    """
    from backend.jobs.sync_bsale_distribuidora import (
        bsale_token_distribuidora_configured,
        run_sync_distribuidora_oc_invoice_background,
    )

    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el proceso.",
        )

    background_tasks.add_task(run_sync_distribuidora_oc_invoice_background)
    return {"status": "sync_oc_invoice_status iniciado"}
