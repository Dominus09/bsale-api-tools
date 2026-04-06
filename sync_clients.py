import requests
import os
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC CLIENTS START")

BASE = "https://api.bsale.io/v1"

LIMIT = 50
BATCH = 500


def parse_coords_from_facebook(fb):
    """
    Espera texto tipo "lat,lon". Devuelve (lat, lon, error_parseo).
    error_parseo es True si había coma pero no se pudo validar.
    """
    if fb is None:
        return None, None, False
    s = str(fb).strip()
    if not s or "," not in s:
        return None, None, False
    try:
        lat_str, lon_str = s.split(",", 1)
        lat = float(lat_str.strip())
        lon = float(lon_str.strip())
    except (ValueError, TypeError) as e:
        print(f"Error parsing facebook coords: {fb!r} -> {e}")
        return None, None, True
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        print(f"Error parsing facebook coords (fuera de rango): {fb!r} -> lat={lat} lon={lon}")
        return None, None, True
    return lat, lon, False


# -----------------------------
# POSTGRES CONNECTION
# -----------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()

cur.execute(
    """
    ALTER TABLE bsale.clients
      ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION,
      ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION
    """
)
conn.commit()


# -----------------------------
# SAFE BSALE REQUEST
# -----------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code == 429:

            wait = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT",wait)
            time.sleep(wait)
            continue

        r.raise_for_status()

        return r.json()


# -----------------------------
# GET COMPANIES
# -----------------------------

def get_companies():

    cur.execute("""

        SELECT company_id,name,bsale_token
        FROM bsale.companies
        WHERE active = true

    """)

    rows = cur.fetchall()

    companies = []

    for r in rows:

        token = os.getenv(r[2])

        if not token:
            print("TOKEN NOT FOUND:", r[2])
            continue

        companies.append({
            "company_id": r[0],
            "name": r[1],
            "token": token
        })

    return companies


# -----------------------------
# UPSERT CLIENTS
# -----------------------------

def upsert(rows):

    execute_batch(cur, """

        INSERT INTO bsale.clients
        (
            company_id,
            bsale_id,
            first_name,
            last_name,
            code,
            phone,
            company,
            facebook,
            city,
            municipality,
            address,
            created,
            updated,
            dia_atencion,
            nombre_fantasia,
            vendedor,
            lat,
            lon
        )

        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET

        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        code = EXCLUDED.code,
        phone = EXCLUDED.phone,
        company = EXCLUDED.company,
        facebook = EXCLUDED.facebook,
        city = EXCLUDED.city,
        municipality = EXCLUDED.municipality,
        address = EXCLUDED.address,
        updated = EXCLUDED.updated,
        dia_atencion = EXCLUDED.dia_atencion,
        nombre_fantasia = EXCLUDED.nombre_fantasia,
        vendedor = EXCLUDED.vendedor,
        lat = CASE
            WHEN EXCLUDED.lat IS NOT NULL AND EXCLUDED.lon IS NOT NULL
            THEN EXCLUDED.lat
            ELSE bsale.clients.lat
        END,
        lon = CASE
            WHEN EXCLUDED.lat IS NOT NULL AND EXCLUDED.lon IS NOT NULL
            THEN EXCLUDED.lon
            ELSE bsale.clients.lon
        END

    """, rows)

    conn.commit()


# -----------------------------
# MAIN
# -----------------------------

companies = get_companies()

total_clients = 0
total_georef_ok = 0
total_georef_error = 0

for company in companies:

    company_id = company["company_id"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE = {"access_token":company["token"]}

    offset = 0
    rows = []
    contador_ok = 0
    contador_error = 0
    contador_procesados = 0

    while True:

        data = bsale_get(
            f"{BASE}/clients.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items = data["items"]

        if not items:
            break

        for client in items:

            bsale_id = client["id"]
            contador_procesados += 1

            created = None
            updated = None

            if client.get("createdAt"):
                created = datetime.fromtimestamp(
                    int(client["createdAt"])
                )

            if client.get("updatedAt"):
                updated = datetime.fromtimestamp(
                    int(client["updatedAt"])
                )


            # ---------------------
            # CLIENT ATTRIBUTES
            # ---------------------

            attr_data = bsale_get(
                f"{BASE}/clients/{bsale_id}/attributes.json",
                HEAD_BSALE
            )

            dia_atencion = None
            nombre_fantasia = None
            vendedor = None

            for attr in attr_data.get("items", []):

                name = attr.get("name","").strip()
                value = attr.get("value")

                if name == "Dia Atencion":
                    dia_atencion = value

                elif name == "NOMBRE DE FANTASÍA":
                    nombre_fantasia = value

                elif name == "Vendedor":
                    vendedor = value

            fb = client.get("facebook")
            lat, lon, parse_err = parse_coords_from_facebook(fb)
            if lat is not None and lon is not None:
                contador_ok += 1
            elif parse_err:
                contador_error += 1

            rows.append((
                company_id,
                bsale_id,
                client.get("firstName"),
                client.get("lastName"),
                client.get("code"),
                client.get("phone"),
                client.get("company"),
                fb,
                client.get("city"),
                client.get("municipality"),
                client.get("address"),
                created,
                updated,
                dia_atencion,
                nombre_fantasia,
                vendedor,
                lat,
                lon
            ))

            if len(rows) >= BATCH:

                upsert(rows)
                rows = []

        offset += LIMIT

    if rows:
        upsert(rows)

    print(f"  Clientes procesados: {contador_procesados}")
    print(f"  Clientes con georef: {contador_ok}")
    print(f"  Errores georef: {contador_error}")

    total_clients += contador_procesados
    total_georef_ok += contador_ok
    total_georef_error += contador_error

print("")
print("SYNC CLIENTS COMPLETE")
print(f"Total clientes procesados: {total_clients}")
print(f"Clientes con georef: {total_georef_ok}")
print(f"Errores georef: {total_georef_error}")
