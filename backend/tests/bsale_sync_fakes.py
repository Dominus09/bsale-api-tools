"""Fakes compartidos para tests de sync Bsale (sin red ni PostgreSQL)."""

from __future__ import annotations

from typing import Any, Callable

import requests


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: Any = None,
        headers: dict[str, str] | None = None,
        text: str | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text is not None else ("" if payload is None else str(payload))

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeSession:
    """Devuelve respuestas (o lanza excepciones) en orden."""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if not self.outcomes:
            raise AssertionError("FakeSession sin respuestas")
        out = self.outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return out


Handler = Callable[[str, Any], Any]


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self.conn = conn
        self.rowcount = 0
        self.description: list[tuple] | None = None
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        norm = " ".join(sql.split())
        self.conn.executed.append((norm, params))
        res = self.conn.handler(norm, params) if self.conn.handler else None
        rows: list[tuple] = []
        rowcount = 0
        description = None
        if isinstance(res, dict):
            rows = list(res.get("rows") or [])
            rowcount = int(res.get("rowcount", len(rows)))
            description = res.get("description")
        elif res is not None:
            rows = list(res)
            rowcount = len(rows)
        self._rows = rows
        self.rowcount = rowcount
        self.description = description or ([("col",)] if rows else None)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class FakeConn:
    def __init__(self, handler: Handler | None = None) -> None:
        self.handler = handler
        self.executed: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.autocommit = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def sql_matching(self, fragment: str) -> list[tuple[str, Any]]:
        return [(s, p) for s, p in self.executed if fragment in s]


class FakeBsaleClient:
    """Client con respuestas por endpoint; un valor Exception se lanza al pedir ese endpoint."""

    def __init__(self, items: dict[str, Any], json_by_path: dict[str, Any] | None = None) -> None:
        self.items = items
        self.json_by_path = json_by_path or {}
        self.requested: list[str] = []

    def fetch_all_items(self, path, params=None, **kwargs):
        self.requested.append(path)
        val = self.items.get(path, [])
        if isinstance(val, BaseException):
            raise val
        return list(val)

    def get_json(self, path, params=None):
        self.requested.append(path)
        val = self.json_by_path.get(path, {})
        if isinstance(val, BaseException):
            raise val
        return val


TIMEOUT_EXC = requests.Timeout("read timeout")
