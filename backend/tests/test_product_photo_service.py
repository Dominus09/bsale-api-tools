import io
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from PIL import Image

from backend.services import product_photo_service as p
from backend.services import system_feature_controls as controls


@pytest.fixture(autouse=True)
def environment_defaults_without_database(monkeypatch):
    # This is image/GitHub unit QA. DB gates are covered in PostgreSQL tests.
    monkeypatch.setattr(controls, "enabled", lambda key, **_: controls.env_default(key))



def image_bytes(fmt="PNG"):
    out = io.BytesIO()
    Image.new("RGB", (640, 320), (190, 20, 50)).save(out, fmt)
    return out.getvalue()


def test_conversion_centers_without_distortion_and_validates_webp():
    data = p.prepare_photo(image_bytes(), "white")
    p.validate_prepared(data)
    with Image.open(io.BytesIO(data)) as img:
        assert img.size == (1000, 1000)
        assert img.format == "WEBP"
        r, g, b = img.convert("RGB").getpixel((500, 500))
        assert r > g * 3
        assert min(img.convert("RGB").getpixel((10, 10))) > 240


@pytest.mark.parametrize("bad", ["../photo", "abc.webp", "foo/bar", ""])
def test_barcode_does_not_allow_path_traversal(bad):
    with pytest.raises(HTTPException):
        p.safe_barcode(bad)


def test_bad_input_and_disabled_ai(monkeypatch):
    with pytest.raises(HTTPException):
        p.prepare_photo(b"not an image", "original")
    monkeypatch.delenv("CATALOG_PHOTO_AI_ENABLED", raising=False)
    with pytest.raises(HTTPException) as exc:
        p.prepare_photo(image_bytes(), "remove")
    assert exc.value.status_code == 503


def test_token_binds_exact_content_product_user_and_sha(monkeypatch):
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "test-secret-" * 4)
    data = p.prepare_photo(image_bytes(), "original")
    args = dict(user_id=42, row_id=8, barcode="00123", variant_id=500, raw=data)
    token = p.sign_preview(**args, old_sha="a" * 40)
    assert p.verify_preview(token, **args) == "a" * 40
    for change in ({"user_id": 43}, {"row_id": 9}, {"barcode": "00456"}, {"variant_id": 501}, {"raw": b"changed"}):
        with pytest.raises(HTTPException):
            p.verify_preview(token, **(args | change))


def test_publish_is_disabled_and_concurrent_replacement_stops(monkeypatch):
    data = p.prepare_photo(image_bytes(), "original")
    args = dict(barcode="00123", raw=data, expected_sha="a"*40, audit_id="f"*32)
    monkeypatch.delenv("CATALOG_PHOTO_WRITES_ENABLED", raising=False)
    with patch.object(p, "_request") as request:
        with pytest.raises(HTTPException):
            p.publish_photo(**args)
        request.assert_not_called()
    monkeypatch.setenv("CATALOG_PHOTO_WRITES_ENABLED", "1")
    monkeypatch.setenv("CATALOG_PHOTO_GITHUB_TOKEN", "test-token")
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "test-secret-" * 4)
    with patch.object(p, "photo_sha", return_value="b"*40), patch.object(p, "_request") as request:
        with pytest.raises(HTTPException) as exc:
            p.publish_photo(**args)
        assert exc.value.status_code == 409
        request.assert_not_called()

def test_publish_mocked_github_success_exact_bytes(monkeypatch):
    import base64
    import hashlib
    from types import SimpleNamespace
    monkeypatch.setenv("CATALOG_PHOTO_WRITES_ENABLED", "1")
    monkeypatch.setenv("CATALOG_PHOTO_GITHUB_TOKEN", "test-token")
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "test-secret-" * 4)
    raw = p.prepare_photo(image_bytes(), "original")
    expected_blob = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
    response = SimpleNamespace(status_code=201, json=lambda: {
        "commit": {"sha": "c"*40}, "content": {"sha": expected_blob},
    })
    with patch.object(p, "photo_sha", return_value=None), patch.object(p, "_request", return_value=response) as request:
        result = p.publish_photo(barcode="00123", raw=raw, expected_sha=None, audit_id="f"*32)
    assert result["new_sha"] == expected_blob
    assert result["catalog_verified"] is False
    payload = request.call_args.kwargs["json"]
    assert "sha" not in payload
    assert base64.b64decode(payload["content"]) == raw
    assert "user=" not in payload["message"]
    assert payload["branch"] == "main"


def test_expired_preview_rejected(monkeypatch):
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "test-secret-" * 4)
    data = p.prepare_photo(image_bytes(), "original")
    args = dict(user_id=42, row_id=8, barcode="00123", variant_id=500, raw=data)
    with patch.object(p.time, "time", return_value=100):
        token = p.sign_preview(**args, old_sha=None)
    with patch.object(p.time, "time", return_value=100 + p.TTL + 1):
        with pytest.raises(HTTPException) as exc:
            p.verify_preview(token, **args)
        assert exc.value.status_code == 409

@pytest.mark.parametrize("raw,expected", [
    (None, True), ("", True), ("/products/00123.webp", True),
    ("https://cat.quillotana.cl/products/00123.webp", True),
    ("/placeholder2.webp", True), ("/products/00456.webp", False),
    ("https://example.com/product.jpg", False),
])
def test_catalog_image_mapping_must_be_compatible(raw, expected):
    assert p.catalog_url_is_compatible(raw, "00123") is expected

def test_heic_can_be_converted_without_browser_support():
    pillow_heif = pytest.importorskip("pillow_heif")
    pillow_heif.register_heif_opener()
    out = io.BytesIO()
    Image.new("RGB", (320, 180), (180, 30, 40)).save(out, format="HEIF")
    prepared = p.prepare_photo(out.getvalue(), "white")
    p.validate_prepared(prepared)
    with Image.open(io.BytesIO(prepared)) as img:
        assert img.format == "WEBP"
        assert img.size == (1000, 1000)


def _sample_with_sidebar():
    image = Image.new("RGB", (200, 200), "white")
    from PIL import ImageDraw
    draw = ImageDraw.Draw(image)
    draw.rectangle((30, 20, 65, 179), fill=(100, 35, 20))
    draw.rectangle((110, 0, 199, 199), fill=(240, 90, 10))
    out = io.BytesIO()
    image.save(out, "PNG")
    return out.getvalue()


def test_manual_crop_excludes_side_banner_and_enlarges_bottle():
    raw = _sample_with_sidebar()
    data = p.prepare_photo(raw, "white", '{"left":0.1,"top":0.05,"right":0.4,"bottom":0.95}')
    p.validate_prepared(data)
    with Image.open(io.BytesIO(data)) as im:
        im = im.convert("RGB")
        # The brown subject should extend far beyond its original 160 px height.
        assert im.getpixel((500, 150))[0] < 150
        assert im.getpixel((500, 850))[0] < 150
        assert min(im.getpixel((950, 500))) > 240
        assert min(im.getpixel((50, 500))) > 240
        # No orange sidebar survived the selection.
        assert not any(r > 210 and 30 < g < 150 and b < 50
                       for r, g, b in im.resize((100, 100)).getdata())


def test_small_source_scaled_without_distortion():
    out = io.BytesIO()
    Image.new("RGB", (100, 200), (100, 35, 20)).save(out, "PNG")
    data = p.prepare_photo(out.getvalue(), "white")
    with Image.open(io.BytesIO(data)) as im:
        im = im.convert("RGB")
        assert im.getpixel((500, 100))[0] < 150
        assert im.getpixel((300, 500))[0] < 150
        assert min(im.getpixel((150, 500))) > 240


@pytest.mark.parametrize("crop", [
    "not json", "null", "[]", '{"left":0}',
    '{"left":0,"top":0,"right":1.1,"bottom":1}',
    '{"left":0.5,"top":0,"right":0.4,"bottom":1}',
    '{"left":0,"top":0,"right":0.01,"bottom":1}',
    '{"left":false,"top":0,"right":1,"bottom":1}',
    '{"left":NaN,"top":0,"right":1,"bottom":1}',
])
def test_bad_crop_rejected(crop):
    with pytest.raises(HTTPException) as exc:
        p.prepare_photo(_sample_with_sidebar(), "white", crop)
    assert exc.value.status_code == 422


def test_source_preview_preserves_exif_orientation_for_manual_crop():
    image = Image.new("RGB", (1600, 800), "white")
    exif = Image.Exif()
    exif[274] = 6
    raw = io.BytesIO()
    image.save(raw, "JPEG", exif=exif)
    with Image.open(io.BytesIO(p.source_preview(raw.getvalue()))) as preview:
        assert preview.size == (600, 1200)
    with Image.open(io.BytesIO(p.prepare_photo(raw.getvalue(), "original"))) as result:
        assert result.getchannel("A").getbbox() == (275, 50, 725, 950)
