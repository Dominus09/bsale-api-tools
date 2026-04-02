from fastapi import HTTPException


def clean_rut_for_lookup(raw_rut: str) -> str:
    return raw_rut.replace(".", "").lower()


def require_valid_rut(rut: str | None) -> str:
    raw = rut.strip() if rut else ""
    if not raw:
        raise HTTPException(status_code=400, detail="RUT vacío")
    if "-" not in raw:
        raise HTTPException(status_code=400, detail="RUT debe incluir guion")
    return clean_rut_for_lookup(raw)


def city_is_melinka(city: str | None) -> bool:
    return "melinka" in (city or "").lower()
