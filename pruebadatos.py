import requests
import json
import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_BSALE = (os.getenv("BSALE_TOKEN") or os.getenv("BSALE_TOKEN_SPA") or "").strip()
if not _BSALE:
    raise SystemExit("Defina BSALE_TOKEN o BSALE_TOKEN_SPA en .env o entorno.")

HEADERS = {
    "access_token": _BSALE,
    "Accept": "application/json"
}

# 🔥 PON AQUÍ UNA OC REAL
DOCUMENT_ID = 3713255  # <-- usa el ID que quieras probar


def get(url):
    res = requests.get(url, headers=HEADERS)
    print(f"\n🔗 {url}")
    print("STATUS:", res.status_code)

    if res.status_code == 200:
        return res.json()
    else:
        print(res.text)
        return None


def main():
    base = f"https://api.bsale.io/v1/documents/{DOCUMENT_ID}"

    print(f"\n🚀 ANALIZANDO OC {DOCUMENT_ID}")

    # DOCUMENTO PRINCIPAL
    doc = get(base + ".json")

    print("\n📄 DOCUMENT:")
    print(json.dumps(doc, indent=2))

    # DETAILS
    if doc and doc.get("details"):
        details = get(doc["details"]["href"])
        print("\n📦 DETAILS:")
        print(json.dumps(details, indent=2))

    # ATTRIBUTES
    if doc and doc.get("attributes"):
        attributes = get(doc["attributes"]["href"])
        print("\n🧠 ATTRIBUTES:")
        print(json.dumps(attributes, indent=2))

    # REFERENCES
    if doc and doc.get("references"):
        references = get(doc["references"]["href"])
        print("\n🔗 REFERENCES:")
        print(json.dumps(references, indent=2))

    # PAYMENTS
    if doc and doc.get("payments"):
        payments = get(doc["payments"]["href"])
        print("\n💰 PAYMENTS:")
        print(json.dumps(payments, indent=2))

    # SELLERS
    if doc and doc.get("sellers"):
        sellers = get(doc["sellers"]["href"])
        print("\n👤 SELLERS:")
        print(json.dumps(sellers, indent=2))

    print("\n🔥 FIN ANALISIS")


if __name__ == "__main__":
    main()