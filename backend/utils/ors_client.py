import requests

from backend.utils.config import ORS_API_KEY

BASE_URL = "https://api.openrouteservice.org/v2/directions/driving-car"


def get_route(coordinates: list[list[float]]) -> dict:
    """
    coordinates: [[lon, lat], [lon, lat], ...]
    """
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }

    body = {"coordinates": coordinates}

    response = requests.post(BASE_URL, json=body, headers=headers)

    if response.status_code != 200:
        raise Exception(f"ORS error: {response.text}")

    return response.json()
