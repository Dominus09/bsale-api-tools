from fastapi import APIRouter
from backend.database import noco_get

router = APIRouter(
    prefix="/auth",
    tags=["Auth"]
)

TABLE_USERS = "mu1cx8k25nmqmox"


@router.post("/login")
def login(data: dict):

    username = str(data.get("username","")).strip()
    password = str(data.get("password","")).strip()

    rows = noco_get(
        TABLE_USERS,
        params={
            "where": f"(username,eq,{username})"
        }
    )

    if not rows:
        return {"ok": False}

    user = rows[0]

    if not user.get("active"):
        return {"ok": False}

    if user["password"] != password:
        return {"ok": False}

    return {
        "ok": True,
        "user": {
            "username": user["username"],
            "role": user["role"]
        }
    }
