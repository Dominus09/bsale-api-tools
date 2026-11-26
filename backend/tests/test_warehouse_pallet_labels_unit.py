"""Pruebas de QR real y contrato de lectura de etiquetas."""
import pytest
from pydantic import ValidationError

from backend.services.warehouse.pallet_labels import make_pallet_label
from backend.services.warehouse.pallet_models import PalletLabelsIn


def record(pid=1, **changes):
    return {
        "id":pid,"code":f"PAL-{pid:06d}","status":"active","pallet_kind":"complete",
        "row_version":1,"received_on":None,"position_code_snap":"P7-01","observed_level":1,
        "notes":"No imprimir observaciones privadas","floor_id":1,
        "lines":[{
            "variant_id":100,"product_name_snap":"SIDRA REAL",
            "variant_name_snap":"BLANCA 750 CC (SEC 6)","product_type_snap":"ESPUMANTES",
            "units_per_box_snap":6,"quantity_status":"unknown","boxes":None,"loose_units":None,
            "total_units":None,"expiry_status":"dated","expires_on":"2029-02-20","lot":None,
            "barcode_snap":"12345678","id":10,
        }],**changes,
    }


def test_label_qr_is_identity_not_location_or_mutable_contents():
    original=record()
    a=make_pallet_label(original)
    b=make_pallet_label({**original,"position_code_snap":"P1-09","observed_level":2,
                         "row_version":3,"lines":[{**original["lines"][0],"expires_on":"2030-02-20"}]})
    assert a["qr_value"]==b["qr_value"]=="PAL-000001"
    assert a["qr_matrix"]==b["qr_matrix"]
    assert a["lines"][0]["expires_on"]!=b["lines"][0]["expires_on"]
    assert a["lines"][0]["total_units"] is None
    assert set(a)=={"id","code","pallet_kind","row_version","received_on","lines","qr_value","qr_matrix"}
    assert "barcode_snap" not in a["lines"][0]
    assert "position_code_snap" not in a and "notes" not in a


def test_qr_matrix_square_quiet_zone_and_distinct_identity():
    a=make_pallet_label(record())
    b=make_pallet_label(record(1000001))
    assert a["qr_matrix"]!=b["qr_matrix"]
    assert b["qr_value"]=="PAL-1000001"
    for m in (a["qr_matrix"],b["qr_matrix"]):
        n=len(m)
        assert n>=29 and (n-29)%4==0
        assert all(len(row)==n and set(row)<=set("01") for row in m)
        assert all(row=="0"*n for row in m[:4]+m[-4:])
        assert all(row[:4]==row[-4:]=="0000" for row in m)
    with pytest.raises(ValueError):
        make_pallet_label(record(status="voided"))
    with pytest.raises(ValueError):
        make_pallet_label(record(code="PAL-000002"))


def test_label_selection_contract():
    assert PalletLabelsIn(floor_id=1,pallet_ids=[1,2]).pallet_ids==[1,2]
    for ids in ([],[1,1],[0],[True],[1.5],list(range(1,122))):
        with pytest.raises(ValidationError):
            PalletLabelsIn(floor_id=1,pallet_ids=ids)
    with pytest.raises(ValidationError):
        PalletLabelsIn(floor_id=1,pallet_ids=[1],position_id=1)


@pytest.mark.parametrize("pid",[1,10,1000001])
def test_qr_independent_decode(pid):
    # Opcional en desarrollo; obligatorio en revisión de este PR:
    # instalar opencv-python-headless en el entorno de QA, no producción.
    cv2=pytest.importorskip("cv2")
    np=pytest.importorskip("numpy")
    label=make_pallet_label(record(pid))
    pixels=np.asarray([[0 if c=="1" else 255 for c in row] for row in label["qr_matrix"]],dtype=np.uint8)
    image=pixels.repeat(10,axis=0).repeat(10,axis=1)
    value,_,_=cv2.QRCodeDetector().detectAndDecode(image)
    assert value==label["code"]
