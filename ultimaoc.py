import requests
import json
import time

BSALE_TOKEN = "3d46d0ac6f42455660f2504d27399d5da3550e25"

HEADERS = {
    "access_token": BSALE_TOKEN,
    "Content-Type": "application/json"
}

BASE_URL = "https://api.bsale.io/v1"

DOC_IDS = [3705336, 3716264]


def get(url):
    res = requests.get(url, headers=HEADERS)
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Error {res.status_code}: {url}")
        return None


def analizar_oc(doc_id):
    print("\n" + "="*50)
    print(f"📦 ANALIZANDO OC ID: {doc_id}")

    # 1. Traer documento
    doc = get(f"{BASE_URL}/documents/{doc_id}.json")
    if not doc:
        return

    print(f"OC Número: {doc['number']}")

    # 2. Traer details
    details = get(f"{BASE_URL}/documents/{doc_id}/details.json")
    if not details or not details.get("items"):
        print("❌ No tiene details")
        return

    print(f"Cantidad de líneas: {len(details['items'])}")

    # 3. tomar primer detail
    detail = details["items"][0]
    detail_id = detail["id"]

    print(f"🔹 Usando detail_id: {detail_id}")

    time.sleep(0.3)

    # 4. buscar documentos relacionados
    related = get(f"{BASE_URL}/documents.json?relateddetailid={detail_id}")

    if not related:
        print("❌ No respondió búsqueda relateddetailid")
        return

    print(f"🔎 Documentos relacionados encontrados: {len(related.get('items', []))}")

    if related.get("items"):
        print("\n🔥 DOCUMENTOS RELACIONADOS:")
        for d in related["items"]:
            print(f"- ID: {d['id']} | Tipo: {d['document_type']['id']} | Número: {d['number']} | Total: {d['totalAmount']}")
    else:
        print("⚠️ No hay documentos relacionados → NO facturada")


def main():
    for doc_id in DOC_IDS:
        analizar_oc(doc_id)


if __name__ == "__main__":
    main()