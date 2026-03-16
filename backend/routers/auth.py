from fastapi import APIRouter, HTTPException
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
