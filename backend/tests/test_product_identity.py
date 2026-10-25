"""Identificación uniforme producto + variante + tipo + barras."""

from __future__ import annotations

from backend.services.analytics_v2.product_identity import (
    BARCODE_MISSING,
    VARIANT_MISSING,
    format_barcode,
    format_product_plus_variant,
    format_product_type,
    identity_fields,
)


def test_product_plus_variant_keeps_presentation():
    assert (
        format_product_plus_variant("CRISTAL", "LATA 470 CC (SEC 24)")
        == "CRISTAL — LATA 470 CC (SEC 24)"
    )


def test_same_product_name_distinct_variants():
    a = format_product_plus_variant("HARINA MARIPOSA", "T/ USO PAPEL 25 KG (SEC 1)")
    b = format_product_plus_variant("HARINA MARIPOSA", "T/ USO PAPEL 25 KG (SEC 6)")
    assert a != b
    assert "SEC 1" in a and "SEC 6" in b


def test_sec_presentations_preserved():
    for sec in ("SEC 1", "SEC 6", "SEC 12", "SEC 24"):
        label = format_product_plus_variant("COCA-COLA", f"LATA 350 CC ({sec})")
        assert sec in label
        assert "COCA-COLA —" in label


def test_variant_missing_explicit():
    assert format_product_plus_variant("Cristal", None).endswith(VARIANT_MISSING)
    assert format_product_plus_variant("Cristal", "  ").endswith(VARIANT_MISSING)
    assert VARIANT_MISSING in format_product_plus_variant("Cristal", "")


def test_barcode_always_text_and_missing_label():
    assert format_barcode(78011612111) == "78011612111"
    assert format_barcode("078011612111") == "078011612111"
    assert format_barcode(None) == BARCODE_MISSING
    assert format_barcode("") == BARCODE_MISSING


def test_identity_fields_bundle():
    fields = identity_fields(
        product_type_name="CERVEZA",
        product_name="CRISTAL",
        variant_name="LATA 470 CC (SEC 24)",
        barcode="78011612111",
    )
    assert fields["product_type"] == "CERVEZA"
    assert fields["product_plus_variant"] == "CRISTAL — LATA 470 CC (SEC 24)"
    assert fields["barcode"] == "78011612111"
    assert format_product_type(None) == "Sin tipo"


def test_company_scoped_identity_does_not_merge_by_name():
    """Contrato: la identidad se arma por campos ya filtrados por company_id/variant_id.

    No hay matching por nombre: dos empresas con el mismo product_name no se mezclan
    porque el caller entrega filas ya acotadas por empresa.
    """
    emp3 = identity_fields(
        product_type_name="CERVEZA",
        product_name="CRISTAL",
        variant_name="SEC 24",
        barcode="780-A",
    )
    emp9 = identity_fields(
        product_type_name="CERVEZA",
        product_name="CRISTAL",
        variant_name="SEC 6",
        barcode="780-B",
    )
    assert emp3["product_plus_variant"] != emp9["product_plus_variant"]
    assert emp3["barcode"] != emp9["barcode"]
