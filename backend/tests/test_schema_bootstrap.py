"""Bootstrap del schema distribuidora sin PostgreSQL: simulación estática del runner.

Se recorren ``DISTRIBUIDORA_SCHEMA_FILES`` en el orden real, troceados igual que
``_run_sql_file``, siguiendo CREATE/DROP (con CASCADE) y referencias ``distribuidora.*``:

* desde una base vacía, ningún statement referencia un objeto inexistente;
* re-ejecutar sobre el resultado (producción ya migrada) no falla y converge al mismo
  catálogo (mismos objetos, misma definición efectiva de cada vista);
* las vistas comerciales efectivas usan la relación resuelta (048).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from backend.repositories.distribuidora import sync_repo
from backend.repositories.distribuidora.schema_preconditions import (
    REISSUE_SCHEMA_OBJECTS,
    RUNNER_REQUIRED_OBJECTS,
)
from backend.repositories.distribuidora.sync_repo import DISTRIBUIDORA_SCHEMA_FILES

SQL_DIR = Path(sync_repo.__file__).resolve().parents[2] / "sql" / "distribuidora"

# Archivos del directorio que no forman parte del runner (y por qué).
NOT_IN_RUNNER = {
    "010_visitas_snapshot_telefonica.sql": "schema bsale (app móvil), no distribuidora",
    "026_MANUAL_pgAdmin_apply_and_validate.sql": "script manual pgAdmin; el runner aplica 026 + 048",
    "031_delivery_day_detect.sql": "funciones opcionales sin uso en el código",
}

LINEAGE_VIEWS = ("v_document_detail_lineage", "v_document_related_resolved")
RECREATED_COMMERCIAL_VIEWS = {
    "v_orders_purchase_status": "003_views.sql",
    "v_orders": "007_document_related_sync_status_views.sql",
    "v_dispatch_plan_invoiced_documents": "026_dispatch_plan_invoiced_view_perf.sql",
}
REISSUE_FILE = "048_document_reissue_lineage.sql"
ROLES_FILE = "049_document_type_roles.sql"


# --------------------------------------------------------------------- lexer


_DOLLAR_RE = re.compile(r"\$(\w*)\$")


def _strip(sql: str) -> str:
    """Quita comentarios y literales '...' (también dentro de cuerpos $tag$)."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        if sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                if sql[j] == "'":
                    break
                j += 1
            out.append("''")
            i = j + 1
            continue
        m = _DOLLAR_RE.match(sql, i)
        if m:
            tag = m.group(0)
            end = sql.find(tag, m.end())
            end = n if end < 0 else end
            out.append(tag + _strip(sql[m.end():end]) + tag)
            i = end + len(tag)
            continue
        out.append(sql[i])
        i += 1
    return "".join(out)


def _split_statements(sql: str) -> list[str]:
    stmts: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        m = _DOLLAR_RE.match(sql, i)
        if m:
            tag = m.group(0)
            end = sql.find(tag, m.end())
            end = n if end < 0 else end + len(tag)
            buf.append(sql[i:end])
            i = end
            continue
        if sql[i] == ";":
            stmts.append("".join(buf))
            buf = []
        else:
            buf.append(sql[i])
        i += 1
    stmts.append("".join(buf))
    return [" ".join(s.split()) for s in stmts if s.strip()]


def _runner_statements(name: str, text: str) -> list[tuple[str, str]]:
    """(origen, statement) en el orden en que el runner los envía."""
    out: list[tuple[str, str]] = []
    for idx, chunk in enumerate(sync_repo._STMT_SPLIT_GO.split(text), start=1):
        if not chunk.strip() or not sync_repo._sql_chunk_has_executable_sql(chunk):
            continue
        for j, stmt in enumerate(_split_statements(_strip(chunk)), start=1):
            out.append((f"{name}#{idx}.{j}", stmt))
    return out


# ------------------------------------------------------------------ catalog


_CREATE_RE = re.compile(
    r"^CREATE\s+(?P<replace>OR\s+REPLACE\s+)?(?:UNIQUE\s+)?(?P<kind>MATERIALIZED\s+VIEW|TABLE|VIEW|FUNCTION|SEQUENCE|TYPE|INDEX)"
    r"\s+(?:CONCURRENTLY\s+)?(?P<ine>IF\s+NOT\s+EXISTS\s+)?(?:distribuidora\.)?(?P<name>\w+)",
    re.I,
)
_DROP_RE = re.compile(
    r"^DROP\s+(?P<kind>MATERIALIZED\s+VIEW|VIEW|TABLE|FUNCTION|INDEX|TYPE|SEQUENCE)\s+(?P<ie>IF\s+EXISTS\s+)?"
    r"(?:distribuidora\.)?(?P<name>\w+)(?P<rest>.*)$",
    re.I,
)
_REF_RE = re.compile(r"distribuidora\.(\w+)", re.I)
_GUARDED_DO_RE = re.compile(r"\bIF\s+(?:NOT\s+)?EXISTS\s*\(|to_regclass", re.I)
_RENAME_RE = re.compile(r"ALTER\s+TABLE\s+distribuidora\.(\w+)\s+RENAME\s+TO\s+(\w+)", re.I)
_DROP_TARGET_RE = re.compile(r"DROP\s+(?:INDEX|VIEW|TABLE|FUNCTION)\s+IF\s+EXISTS\s+distribuidora\.(\w+)", re.I)


@dataclass
class Obj:
    kind: str
    source: str
    deps: frozenset[str] = frozenset()


@dataclass
class Catalog:
    objects: dict[str, Obj] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def snapshot(self) -> dict[str, tuple[str, str | None, frozenset[str]]]:
        return {
            k: (o.kind, o.source if o.kind in ("view", "function") else None, o.deps)
            for k, o in self.objects.items()
        }

    def _exists(self, name: str) -> bool:
        if name in self.objects:
            return True
        # Secuencias implícitas de SERIAL/BIGSERIAL.
        return name.endswith("_seq") and any(name.startswith(t + "_") for t in self.objects)

    def _dependents(self, name: str) -> set[str]:
        out: set[str] = set()
        frontier = {name}
        while frontier:
            nxt = {k for k, o in self.objects.items() if o.deps & frontier and k not in out}
            out |= nxt
            frontier = nxt
        return out

    def _check_refs(self, origin: str, refs: set[str]) -> None:
        for r in sorted(refs):
            if not self._exists(r):
                self.errors.append(f"{origin}: referencia distribuidora.{r} inexistente")

    def apply(self, origin: str, stmt: str) -> None:
        body = stmt
        is_do = re.match(r"^DO\b", stmt, re.I) is not None
        is_plpgsql_fn = (
            re.match(r"^CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b", stmt, re.I)
            and re.search(r"LANGUAGE\s+plpgsql", stmt, re.I)
        )
        if is_plpgsql_fn:
            body = re.sub(r"\$(\w*)\$.*?\$\1\$", "", stmt, flags=re.S)
        refs = {r.lower() for r in _REF_RE.findall(body)}
        refs -= {t.lower() for t in _DROP_TARGET_RE.findall(body)}

        if is_do:
            if _GUARDED_DO_RE.search(body):
                # Rama condicional: sus referencias se evalúan solo si el guard se cumple. Los
                # guards existentes protegen renombres de tablas legadas (origen sí, destino no).
                for src, dst in _RENAME_RE.findall(body):
                    src, dst = src.lower(), dst.lower()
                    if src in self.objects and dst not in self.objects:
                        self.objects[dst] = self.objects.pop(src)
                return
            for m in re.finditer(r"CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?distribuidora\.(\w+)", body, re.I):
                self.objects.setdefault(m.group(2).lower(), Obj(m.group(1).lower(), origin))
            self._check_refs(origin, refs - set(self.objects))
            return

        m = _CREATE_RE.match(stmt)
        if m:
            kind = re.sub(r"\s+", " ", m.group("kind").lower())
            name = m.group("name").lower()
            if kind == "index":
                self._check_refs(origin, refs)
                return
            refs.discard(name)
            if kind == "materialized view":
                kind = "view"
            exists = name in self.objects
            if exists and m.group("ine"):
                return
            if exists and not m.group("replace"):
                self.errors.append(f"{origin}: CREATE {kind} {name} ya existe (sin OR REPLACE / IF NOT EXISTS)")
                return
            self._check_refs(origin, refs)
            deps = frozenset(refs) if kind == "view" else frozenset()
            self.objects[name] = Obj(kind, origin, deps)
            return

        m = _DROP_RE.match(stmt)
        if m:
            kind = m.group("kind").lower()
            if kind == "index":
                return
            name = m.group("name").lower()
            if name not in self.objects:
                if not m.group("ie"):
                    self.errors.append(f"{origin}: DROP {kind} {name} inexistente sin IF EXISTS")
                return
            dependents = self._dependents(name)
            if dependents and "cascade" not in m.group("rest").lower():
                self.errors.append(f"{origin}: DROP {name} sin CASCADE con dependientes {sorted(dependents)}")
                return
            for d in dependents | {name}:
                self.objects.pop(d, None)
            return

        self._check_refs(origin, refs)


def simulate(files: list[tuple[str, str]], catalog: Catalog | None = None) -> Catalog:
    cat = catalog or Catalog()
    for name, text in files:
        for origin, stmt in _runner_statements(name, text):
            cat.apply(origin, stmt)
    return cat


def _runner_files() -> list[tuple[str, str]]:
    return [(n, (SQL_DIR / n).read_text(encoding="utf-8")) for n in DISTRIBUIDORA_SCHEMA_FILES]


@pytest.fixture(scope="module")
def runs():
    files = _runner_files()
    fresh = simulate(files)
    first = fresh.snapshot()
    rerun = simulate(files, Catalog(objects=dict(fresh.objects)))
    return fresh, first, rerun


# -------------------------------------------------------------------- tests


def test_every_sql_file_is_registered_or_explicitly_excluded():
    on_disk = {p.name for p in SQL_DIR.glob("*.sql")}
    registered = set(DISTRIBUIDORA_SCHEMA_FILES)
    assert registered <= on_disk
    assert sorted(on_disk - registered) == sorted(NOT_IN_RUNNER)
    assert list(DISTRIBUIDORA_SCHEMA_FILES) == sorted(DISTRIBUIDORA_SCHEMA_FILES)
    assert len(set(DISTRIBUIDORA_SCHEMA_FILES)) == len(DISTRIBUIDORA_SCHEMA_FILES)
    assert DISTRIBUIDORA_SCHEMA_FILES[-2:] == (REISSUE_FILE, ROLES_FILE)


def test_bootstrap_from_empty_database_has_no_forward_references(runs):
    fresh, _, _ = runs
    assert fresh.errors == []


def test_rerun_over_migrated_database_is_idempotent_and_converges(runs):
    fresh, first, rerun = runs
    assert rerun.errors == []
    assert rerun.snapshot() == first


def test_runner_required_objects_exist_after_fresh_and_rerun(runs):
    fresh, _, rerun = runs
    for cat in (fresh, rerun):
        for qualified in RUNNER_REQUIRED_OBJECTS:
            assert qualified.split(".", 1)[1] in cat.objects, qualified
        roles = cat.objects["document_type_roles"]
        assert roles.kind == "table" and roles.source.startswith(ROLES_FILE)


def test_reissue_objects_exist_after_bootstrap(runs):
    fresh, _, _ = runs
    for qualified in REISSUE_SCHEMA_OBJECTS:
        name = qualified.split(".", 1)[1]
        assert name in fresh.objects, name
    assert fresh.objects["document_detail_history"].kind == "table"
    for v in LINEAGE_VIEWS:
        assert fresh.objects[v].kind == "view"
        assert fresh.objects[v].source.startswith(REISSUE_FILE)


def test_commercial_views_effective_definition_comes_from_reissue_file(runs):
    fresh, _, rerun = runs
    for cat in (fresh, rerun):
        for view in RECREATED_COMMERCIAL_VIEWS:
            assert cat.objects[view].source.startswith(REISSUE_FILE), view
            assert "v_document_related_resolved" in cat.objects[view].deps, view


_LEGACY_JOIN_RE = re.compile(
    r"document_details\s+\w+\s+(?:INNER\s+)?JOIN\s+distribuidora\.document_related"
    r"|document_related\s+\w+\s+(?:INNER\s+)?JOIN\s+distribuidora\.document_details",
    re.I,
)


def _effective_view_sql(view: str, source: str) -> str:
    file_name = source.split("#", 1)[0]
    for origin, stmt in _runner_statements(file_name, (SQL_DIR / file_name).read_text(encoding="utf-8")):
        if origin == source:
            return stmt
    raise AssertionError(f"{view}: statement {source} no encontrado")


def test_no_effective_view_joins_related_only_to_current_details(runs):
    fresh, _, _ = runs
    offenders = [
        f"{name} ({o.source})"
        for name, o in fresh.objects.items()
        if o.kind == "view" and _LEGACY_JOIN_RE.search(_effective_view_sql(name, o.source))
    ]
    assert offenders == []


def _view_body(file_name: str, view: str) -> str:
    stmts = [
        s
        for _, s in _runner_statements(file_name, (SQL_DIR / file_name).read_text(encoding="utf-8"))
        if re.match(rf"^CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+distribuidora\.{view}\s+AS\b", s, re.I)
    ]
    assert len(stmts) == 1, (file_name, view, len(stmts))
    return re.sub(r"^CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+", "", stmts[0], flags=re.I)


def _as_resolved(legacy: str) -> str:
    out = re.sub(
        r"FROM distribuidora\.document_related dr INNER JOIN distribuidora\.document_details dd "
        r"ON dd\.detail_id = dr\.detail_id",
        "FROM distribuidora.v_document_related_resolved dr",
        legacy,
    )
    out = re.sub(
        r"FROM distribuidora\.document_details dd INNER JOIN distribuidora\.document_related dr "
        r"ON dr\.detail_id = dd\.detail_id",
        "FROM distribuidora.v_document_related_resolved dr",
        out,
    )
    return out.replace("WHERE dd.document_id = ", "WHERE dr.origin_document_id = ")


@pytest.mark.parametrize("view,legacy_file", sorted(RECREATED_COMMERCIAL_VIEWS.items()))
def test_recreated_view_matches_legacy_except_relation_source(view, legacy_file):
    """CREATE OR REPLACE exige mismas columnas; además evita que 048 y el archivo original diverjan."""
    legacy = _view_body(legacy_file, view)
    final = _view_body(REISSUE_FILE, view)
    assert _LEGACY_JOIN_RE.search(legacy), "el original ya no usa el JOIN legacy: actualizar 048 y este test"
    assert final == _as_resolved(legacy)


def test_reissue_file_ddl_is_idempotent_and_non_destructive():
    text = _strip((SQL_DIR / REISSUE_FILE).read_text(encoding="utf-8"))
    upper = text.upper()
    assert "CREATE TABLE IF NOT EXISTS DISTRIBUIDORA.DOCUMENT_DETAIL_HISTORY" in upper
    assert "CREATE INDEX IF NOT EXISTS IDX_DISTRIBUIDORA_DETAIL_HISTORY_DOCUMENT" in upper
    assert "DROP CONSTRAINT IF EXISTS FK_DISTRIBUIDORA_DOCUMENT_RELATED_DETAIL" in upper
    assert "WHERE DOCUMENT_TYPE_ID IS NOT NULL AND NUMBER > 0" in upper
    assert "INDEXDEF LIKE" in upper
    for forbidden in ("DELETE ", "TRUNCATE", "DROP TABLE", "UPDATE DISTRIBUIDORA", "INSERT INTO", " CASCADE;"):
        assert forbidden not in upper.replace("ON DELETE CASCADE", "").replace("ON UPDATE CASCADE", ""), forbidden
    for view in (*LINEAGE_VIEWS, *RECREATED_COMMERCIAL_VIEWS):
        assert f"CREATE OR REPLACE VIEW DISTRIBUIDORA.{view.upper()} AS" in upper


def test_fk_cascade_is_never_recreated_by_runner():
    for name, text in _runner_files():
        assert "ADD CONSTRAINT FK_DISTRIBUIDORA_DOCUMENT_RELATED_DETAIL" not in _strip(text).upper(), name


def test_document_related_created_before_first_reference():
    first_ref = next(
        n for n in DISTRIBUIDORA_SCHEMA_FILES
        if "distribuidora.document_related" in _strip((SQL_DIR / n).read_text(encoding="utf-8"))
    )
    assert "CREATE TABLE IF NOT EXISTS distribuidora.document_related" in (SQL_DIR / first_ref).read_text(
        encoding="utf-8"
    )


# ----------------------------------------------- the checker catches regressions


def test_checker_detects_view_referencing_table_created_later():
    files = [
        ("001_a.sql", "CREATE TABLE IF NOT EXISTS distribuidora.a (id int);\n-- +go\n"),
        ("003_v.sql", "CREATE OR REPLACE VIEW distribuidora.v AS SELECT * FROM distribuidora.b;\n-- +go\n"),
        ("007_b.sql", "CREATE TABLE IF NOT EXISTS distribuidora.b (id int);\n-- +go\n"),
    ]
    fresh = simulate(files)
    assert fresh.errors == ["003_v.sql#1.1: referencia distribuidora.b inexistente"]
    # El mismo layout "funciona" sobre una base que ya tenía b: exactamente el caso a evitar.
    seeded = Catalog(objects={"a": Obj("table", "x"), "b": Obj("table", "x")})
    assert simulate(files, seeded).errors == []


def test_checker_detects_view_lost_by_cascade_without_recreation():
    files = [
        ("001_t.sql", "CREATE TABLE IF NOT EXISTS distribuidora.t (id int);\n-- +go\n"
         "CREATE OR REPLACE VIEW distribuidora.base AS SELECT * FROM distribuidora.t;\n-- +go\n"),
        ("002_v.sql", "CREATE OR REPLACE VIEW distribuidora.child AS SELECT * FROM distribuidora.base;\n-- +go\n"),
        ("003_d.sql", "DROP VIEW IF EXISTS distribuidora.base CASCADE;\n-- +go\n"
         "CREATE OR REPLACE VIEW distribuidora.base AS SELECT * FROM distribuidora.t;\n-- +go\n"),
    ]
    fresh = simulate(files)
    assert fresh.errors == []
    assert "child" not in fresh.objects


def test_checker_detects_drop_without_cascade_with_dependents():
    files = [
        ("001.sql", "CREATE TABLE IF NOT EXISTS distribuidora.t (id int);\n-- +go\n"
         "CREATE OR REPLACE VIEW distribuidora.v1 AS SELECT * FROM distribuidora.t;\n-- +go\n"
         "CREATE OR REPLACE VIEW distribuidora.v2 AS SELECT * FROM distribuidora.v1;\n-- +go\n"
         "DROP VIEW IF EXISTS distribuidora.v1;\n-- +go\n"),
    ]
    assert simulate(files).errors == ["001.sql#4.1: DROP v1 sin CASCADE con dependientes ['v2']"]
