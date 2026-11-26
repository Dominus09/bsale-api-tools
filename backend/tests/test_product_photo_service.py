import io
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from PIL import Image

from backend.services import product_photo_service as p


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
    args = dict(user_id=42, row_id=8, barcode="00123", raw=data)
    token = p.sign_preview(**args, old_sha="a" * 40)
    assert p.verify_preview(token, **args) == "a" * 40
    for change in ({"user_id": 43}, {"row_id": 9}, {"barcode": "00456"}, {"raw": b"changed"}):
        with pytest.raises(HTTPException):
            p.verify_preview(token, **(args | change))


def test_publish_is_disabled_and_concurrent_replacement_stops(monkeypatch):
    data = p.prepare_photo(image_bytes(), "original")
    args = dict(barcode="00123", raw=data, expected_sha="a"*40, user_id=42, row_id=8)
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
