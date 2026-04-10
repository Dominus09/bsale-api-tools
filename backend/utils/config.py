import os

ORS_API_KEY = os.getenv("ORS_API_KEY")

if not ORS_API_KEY:
    raise Exception("ORS_API_KEY no configurada en variables de entorno")
