import psycopg2

try:
    conn = psycopg2.connect(
        host="217.216.89.226",
        database="analytics",
        user="postgres",
        password="mQIAEdnVXzPLszj17Vp93tqceDNEYaEF7ywnuBFyRTDO5A",
        port=5432
    )
    print("✅ Conectado correctamente")
    conn.close()
except Exception as e:
    print("❌ Error:", e)