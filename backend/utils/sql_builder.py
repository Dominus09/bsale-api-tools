"""Constructor SQL con parámetros alineados — evita desajuste de placeholders."""

from __future__ import annotations

from typing import Any


class SqlBuilder:
    """Acumula fragmentos WHERE y parámetros en el mismo orden que los %s."""

    def __init__(self, *, prefix: str = "") -> None:
        self._prefix = prefix.strip()
        self._clauses: list[str] = []
        self._params: list[Any] = []

    def where(self, fragment: str, *values: Any) -> SqlBuilder:
        """Añade un fragmento SQL con placeholders %s y sus valores."""
        expected = fragment.count("%s")
        if expected != len(values):
            raise ValueError(
                f"SqlBuilder.where: fragment has {expected} placeholders, got {len(values)} values"
            )
        self._clauses.append(fragment.strip())
        self._params.extend(values)
        return self

    def extend(self, fragment: str, params: list[Any] | tuple[Any, ...]) -> SqlBuilder:
        """Añade un fragmento ya validado con su lista de parámetros."""
        expected = fragment.count("%s")
        if expected != len(params):
            raise ValueError(
                f"SqlBuilder.extend: fragment has {expected} placeholders, got {len(params)} params"
            )
        self._clauses.append(fragment.strip())
        self._params.extend(params)
        return self

    @property
    def sql(self) -> str:
        if not self._clauses:
            return self._prefix
        body = " AND ".join(self._clauses)
        if self._prefix:
            return f"{self._prefix} AND {body}"
        return body

    @property
    def params(self) -> list[Any]:
        return list(self._params)

    def as_tuple(self) -> tuple[Any, ...]:
        return tuple(self._params)

    def validate_against(self, sql: str) -> None:
        """Comprueba que el SQL completo tenga tantos %s como parámetros acumulados."""
        placeholders = sql.count("%s")
        if placeholders != len(self._params):
            raise ValueError(
                f"SqlBuilder.validate_against: sql has {placeholders} placeholders, "
                f"builder has {len(self._params)} params"
            )
