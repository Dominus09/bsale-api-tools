"""Control de Góndolas: clasificación de escaneos y capacidades por rol (sin base de datos)."""

from decimal import Decimal

from backend.services.gondolas.access import gondola_capabilities
from backend.services.gondolas.service import classify_scan

CANDIDATE = {
    "variant_id": 10,
    "product_id": 5,
    "product_type_id": 35,
    "stock": Decimal("3"),
    "product_name": "Vino Reserva",
    "variant_name": "750 cc",
}


def test_scan_sin_coincidencia():
    result, candidate = classify_scan([], None, product_type_id=35)
    assert result == "sin_coincidencia"
    assert candidate is None


def test_scan_ambiguo_no_elige_variante():
    other = {**CANDIDATE, "variant_id": 11}
    result, candidate = classify_scan([CANDIDATE, other], None, product_type_id=35)
    assert result == "ambiguo"
    assert candidate is None


def test_scan_encontrado_y_duplicado():
    result, _ = classify_scan([CANDIDATE], {"exhibition_status": "pendiente"}, product_type_id=35)
    assert result == "encontrado"
    result, _ = classify_scan([CANDIDATE], {"exhibition_status": "encontrado"}, product_type_id=35)
    assert result == "duplicado"


def test_scan_fuera_de_tipo_y_stock_cero():
    other_type = {**CANDIDATE, "product_type_id": 16}
    result, _ = classify_scan([other_type], None, product_type_id=35)
    assert result == "fuera_de_tipo"

    no_stock = {**CANDIDATE, "stock": Decimal("0")}
    result, _ = classify_scan([no_stock], None, product_type_id=35)
    assert result == "stock_cero"


def test_scan_fuera_de_universo_con_stock():
    result, candidate = classify_scan([CANDIDATE], None, product_type_id=35)
    assert result == "fuera_de_universo"
    assert candidate is CANDIDATE


def test_capacidades_por_rol():
    assert gondola_capabilities({"role": "operativo"}) == {"operate": True, "cancel": False}
    assert gondola_capabilities({"role": "adm"}) == {"operate": True, "cancel": True}
    assert gondola_capabilities({"role": "gerencia"}) == {"operate": True, "cancel": True}
    assert gondola_capabilities({"role": "finanzas"}) == {"operate": False, "cancel": False}
    assert gondola_capabilities(None) == {"operate": False, "cancel": False}
