from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from backend.db import get_connection
from passlib.context import CryptContext
import jwt
import os

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET = "quillotana_secret_key"


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginClientRequest(BaseModel):
    rut: str


def _clean_rut_for_lookup(rut: str) -> str:
    return rut.replace(".", "").lower()


@router.post("/login-client")
def login_client(body: LoginClientRequest):
    raw = body.rut.strip() if body.rut else ""
    if not raw:
        raise HTTPException(status_code=400, detail="RUT vacío")
    if "-" not in raw:
        raise HTTPException(status_code=400, detail="RUT debe incluir guion")

    rut_clean = _clean_rut_for_lookup(raw)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bsale_id, first_name, last_name, city
            FROM bsale.clients
            WHERE rut_clean = %s
            """,
            (rut_clean,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Cliente no encontrado. Contactar al +56 9 9271 4314"
            },
        )

    bsale_id, first_name, last_name, city = row
    city_val = city or ""
    name = f"{first_name or ''} {last_name or ''}".strip()

    return {
        "id": bsale_id,
        "name": name,
        "city": city,
        "is_melinka": "melinka" in city_val.lower(),
    }


@router.post("/login")
def login(data: LoginRequest):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT password_hash, role FROM bsale.users WHERE email = %s AND active = true",
        (data.email,)
    )

    user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    password_hash, role = user

    if not pwd_context.verify(data.password, password_hash):
        raise HTTPException(status_code=401, detail="Password incorrecta")

    token = jwt.encode(
        {"email": data.email, "role": role},
        SECRET,
        algorithm="HS256"
    )

    return {
        "token": token,
        "email": data.email,
        "role": role
    }
