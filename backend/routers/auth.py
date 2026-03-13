from fastapi import APIRouter, HTTPException
from backend.database import noco_get

router = APIRouter(prefix="/auth", tags=["auth"])

TABLE_USERS = "mu1cx8k25nmqmox"


@router.post("/login")
def login(data: dict):

    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="missing credentials")

    rows = noco_get(
        TABLE_USERS,
        params={"where": f"(username,eq,{username})"}
    )

    if not rows:
        raise HTTPException(status_code=401, detail="user not found")

    user = rows[0]

    if not user.get("active"):
        raise HTTPException(status_code=403, detail="user disabled")

    if user["password"] != password:
        raise HTTPException(status_code=401, detail="wrong password")

    return {
        "username": user["username"],
        "role": user["role"]
    }
