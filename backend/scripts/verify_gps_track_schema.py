"""
Valida parsing de POST /operaciones/gps_track (sin BD).

  python -m backend.scripts.verify_gps_track_schema
"""

from __future__ import annotations

from backend.schemas.operaciones import GpsTrackRequest


def main() -> None:
    batch = GpsTrackRequest.model_validate(
        {
            "vendedor_id": "vendedor_1",
            "session_id": "sess-1",
            "timestamp": "2026-05-22T12:00:00+00:00",
            "point_ids": ["p1", "p2"],
            "puntos": [
                {"lat": -33.45, "lon": -70.66, "accuracy_m": 12.5},
                {"lat": -33.46, "lng": -70.67},
            ],
        }
    )
    assert len(batch.puntos) == 2
    assert batch.puntos[0].lng_efectivo() == -70.66

    single = GpsTrackRequest.model_validate(
        {
            "vendedor_id": "vendedor_1",
            "timestamp": "2026-05-22T12:00:00+00:00",
            "lat": -33.45,
            "lng": -70.66,
        }
    )
    assert single.lat == -33.45
    assert single.lng_efectivo() == -70.66

    print("OK: formato batch y single parsean correctamente")


if __name__ == "__main__":
    main()
