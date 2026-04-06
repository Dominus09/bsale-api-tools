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


@router.post("/upload/offers")
def upload_offers(file: UploadFile = File(...)):
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
    required = {"barcode", "offer_type", "status", "start_date", "end_date"}
    if not required.issubset(set(df.columns)):
        raise HTTPException(
            status_code=400,
            detail="Columnas requeridas: barcode, offer_type, status, start_date, end_date",
        )

    for col in ("reason", "notes"):
        if col not in df.columns:
            df[col] = None

    upload_df = df[
        ["barcode", "offer_type", "status", "start_date", "end_date", "reason", "notes"]
    ].copy()

    upload_df["barcode"] = upload_df["barcode"].fillna("").astype(str).str.strip()
    upload_df["offer_type"] = upload_df["offer_type"].fillna("").astype(str).str.strip()
    upload_df["status"] = upload_df["status"].fillna("").astype(str).str.strip()

    start_parsed = pd.to_datetime(upload_df["start_date"], errors="coerce")
    end_parsed = pd.to_datetime(upload_df["end_date"], errors="coerce")
    upload_df["start_date"] = start_parsed.dt.date
    upload_df["end_date"] = end_parsed.dt.date

    upload_df["reason"] = upload_df["reason"].where(upload_df["reason"].notna(), None)
    upload_df["notes"] = upload_df["notes"].where(upload_df["notes"].notna(), None)
    upload_df["reason"] = upload_df["reason"].apply(
        lambda v: None if v is None else (str(v).strip() or None)
    )
    upload_df["notes"] = upload_df["notes"].apply(
        lambda v: None if v is None else (str(v).strip() or None)
    )

    valid_basic_mask = (
        (upload_df["barcode"] != "") &
        (upload_df["offer_type"] != "") &
        (upload_df["status"] != "") &
        (upload_df["start_date"].notna()) &
        (upload_df["end_date"].notna()) &
        (upload_df["start_date"] <= upload_df["end_date"])
    )
    valid_basic_df = upload_df[valid_basic_mask].copy()
    malformed_count = int(len(upload_df) - len(valid_basic_df))

    if valid_basic_df.empty:
        return {
            "inserted": 0,
            "invalid": int(len(upload_df)),
        }

    rows = list(valid_basic_df.itertuples(index=False, name=None))

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TEMP TABLE temp_offer_upload (
                barcode TEXT,
                offer_type TEXT,
                status TEXT,
                start_date DATE,
                end_date DATE,
                reason TEXT,
                notes TEXT
            ) ON COMMIT DROP
            """
        )

        execute_values(
            cur,
            """
            INSERT INTO temp_offer_upload (
                barcode, offer_type, status, start_date, end_date, reason, notes
            ) VALUES %s
            """,
            rows,
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM temp_offer_upload t
            LEFT JOIN bsale.products_master pm
              ON pm.barcode = t.barcode
            WHERE pm.barcode IS NULL
            """
        )
        not_found_count = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO bsale.product_offers (
                barcode,
                offer_type,
                status,
                start_date,
                end_date,
                reason,
                notes
            )
            SELECT
                t.barcode,
                t.offer_type,
                t.status,
                t.start_date,
                t.end_date,
                t.reason,
                t.notes
            FROM temp_offer_upload t
            INNER JOIN bsale.products_master pm
              ON pm.barcode = t.barcode
            """
        )
        inserted_count = int(cur.rowcount)

        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error procesando carga masiva de ofertas")
    finally:
        cur.close()
        conn.close()

    invalid_count = malformed_count + not_found_count
    return {
        "inserted": inserted_count,
        "invalid": invalid_count,
    }
