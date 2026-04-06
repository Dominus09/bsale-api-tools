import io

from fastapi import APIRouter, File, HTTPException, UploadFile
from psycopg2.extras import execute_values

from backend.db import get_connection

router = APIRouter()


@router.post("/upload/suppliers")
def upload_suppliers(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Archivo requerido")

    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="pandas no disponible en el backend",
        )

    raw = file.file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    try:
        df = pd.read_excel(io.BytesIO(raw))
    except Exception:
        raise HTTPException(status_code=400, detail="No se pudo leer el archivo Excel")

    if df.empty:
        raise HTTPException(status_code=400, detail="El archivo no contiene filas")

    df.columns = [str(c).strip().lower() for c in df.columns]
    required = {"barcode", "supplier_id"}
    if not required.issubset(set(df.columns)):
        raise HTTPException(
            status_code=400,
            detail="Columnas requeridas: barcode, supplier_id",
        )

    upload_df = df[["barcode", "supplier_id"]].copy()
    upload_df["barcode"] = upload_df["barcode"].fillna("").astype(str).str.strip()
    upload_df["supplier_id"] = pd.to_numeric(upload_df["supplier_id"], errors="coerce")
    upload_df = upload_df[
        (upload_df["barcode"] != "") &
        (upload_df["supplier_id"].notna())
    ]

    if upload_df.empty:
        raise HTTPException(status_code=400, detail="No hay filas válidas para procesar")

    upload_df["supplier_id"] = upload_df["supplier_id"].astype(int)
    upload_df = upload_df.drop_duplicates(subset=["barcode"], keep="last")

    rows = list(upload_df.itertuples(index=False, name=None))
    if not rows:
        raise HTTPException(status_code=400, detail="No hay filas válidas para procesar")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TEMP TABLE temp_upload (
                barcode TEXT,
                supplier_id INT
            ) ON COMMIT DROP
            """
        )

        execute_values(
            cur,
            "INSERT INTO temp_upload (barcode, supplier_id) VALUES %s",
            rows,
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM temp_upload t
            LEFT JOIN bsale.products_master pm
              ON pm.barcode = t.barcode
            WHERE pm.barcode IS NULL
            """
        )
        not_found_count = int(cur.fetchone()[0])

        cur.execute(
            """
            UPDATE bsale.products_master pm
            SET supplier_id = t.supplier_id,
                updated_at = NOW()
            FROM temp_upload t
            WHERE pm.barcode = t.barcode
            """
        )
        updated_count = int(cur.rowcount)

        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error procesando carga masiva")
    finally:
        cur.close()
        conn.close()

    return {
        "updated_count": updated_count,
        "not_found_count": not_found_count,
        "uploaded_count": len(rows),
    }
