"""Etiquetas de identidad física: QR estándar, sólo código PAL.

El contenido del QR no incluye ubicación, producto, credenciales, datos
comerciales ni un dominio particular. Un lector autorizado consulta la ficha
actual. La zona de silencio de cuatro módulos es parte de la matriz.
"""
from __future__ import annotations

import qrcode
from qrcode.constants import ERROR_CORRECT_M


LABEL_LINE_FIELDS = (
    "variant_id","product_name_snap","variant_name_snap","product_type_snap",
    "units_per_box_snap","quantity_status","boxes","loose_units","total_units",
    "expiry_status","expires_on","lot",
)


def make_pallet_label(record: dict) -> dict:
    code = record["code"]
    if code != f"PAL-{record['id']:06d}" or record["status"] != "active":
        raise ValueError("Identidad activa inválida para impresión.")
    qr = qrcode.QRCode(version=None,error_correction=ERROR_CORRECT_M,box_size=1,border=4)
    qr.add_data(code)
    qr.make(fit=True)
    matrix = ["".join("1" if pixel else "0" for pixel in row) for row in qr.get_matrix()]
    return {
        "id":record["id"],"code":code,"pallet_kind":record["pallet_kind"],
        "row_version":record["row_version"],"received_on":record["received_on"],
        "lines":[{k:line[k] for k in LABEL_LINE_FIELDS} for line in record["lines"]],
        "qr_value":code,"qr_matrix":matrix,
    }
