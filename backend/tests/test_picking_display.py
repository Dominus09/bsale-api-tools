"""Pruebas unitarias: cajas operativas picking (2 decimales, sin CEIL)."""

from backend.utils.picking_display import effective_cajas


def test_cajas_sec_48_diez_unidades():
    assert effective_cajas(10, 48, None, sin_unidad_caja=False) == 0.21


def test_cajas_seis_por_caja():
    assert effective_cajas(18, 6, None, sin_unidad_caja=False) == 3.0


def test_cajas_sec_24():
    assert effective_cajas(720, 24, None, sin_unidad_caja=False) == 30.0


def test_cajas_sin_unidad_caja():
    assert effective_cajas(10, None, 5, sin_unidad_caja=True) == 0.0
