from fastapi import APIRouter, HTTPException
from backend.db import get_connection
from passlib.context import CryptContext
from jose import jwt
import datetime

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET = "QUILLOTANA_SECRET_KEY"


@router.post("/login")
def login(email: str, password: str):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, email, password_hash, role
        FROM bsale.users
        WHERE email = %s
        AND active = true
    """, (email,))

    user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Usuario no existe")

    user_id, email_db, password_hash, role = user

    if not pwd_context.verify(password, password_hash):
        raise HTTPException(status_code=401, detail="Password incorrecta")

    payload = {
        "user_id": user_id,
        "email": email_db,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    }

    token = jwt.encode(payload, SECRET, algorithm="HS256")

    return {
        "token": token,
        "email": email_db,
        "role": role
    }
