"""Tests estáticos de las migraciones ``backend/sql/bsale_raw`` (sin BD; no ejecutan SQL)."""

from __future__ import annotations

import re

import pytest

import backend.services.bsale_raw.resources  # noqa: F401
from backend.services.bsale_raw.core.models import (
    DocumentChangeKind,
    ResponseEnvelope,
    RunStatus,
    SyncMode,
    WebhookStatus,
)
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.tests.bsale_raw._raw_sql_schema import (
    VERIFY_FILE,
    check_in_values,
    migration_files,
    parse,
    render_expected_block,
    strip_comments,
    verify_expected_block,
)

TABLES, INDEXES = parse()

EXPECTED_PKS = {
    "sources": ("company_id",),
    "sync_runs": ("id",),
    "sync_entity_runs": ("id",),
    "sync_state": ("company_id", "resource", "scope"),
    "sync_cursors": ("company_id", "resource", "scope", "cursor_name"),
    "offices": ("company_id", "bsale_id"),
    "taxes": ("company_id", "bsale_id"),
    "document_types": ("company_id", "bsale_id"),
    "product_types": ("company_id", "bsale_id"),
    "price_lists": ("company_id", "bsale_id"),
    "products": ("company_id", "bsale_id"),
    "variants": ("company_id", "bsale_id"),
    "clients": ("company_id", "bsale_id"),
    "stocks": ("company_id", "variant_id", "office_id"),
    "variant_prices": ("company_id", "price_list_id", "variant_id"),
    "variant_costs": ("company_id", "variant_id"),
    "documents": ("company_id", "bsale_id"),
    "document_details": ("company_id", "document_id", "bsale_id"),
    "document_references": ("company_id", "document_id", "bsale_id"),
    "document_sellers": ("company_id", "document_id", "user_id"),
    "document_change_log": ("id",),
    "stock_receptions": ("company_id", "bsale_id"),
    "stock_reception_details": ("company_id", "reception_id", "bsale_id"),
    "stock_consumptions": ("company_id", "bsale_id"),
    "stock_consumption_details": ("company_id", "consumption_id", "bsale_id"),
    "webhook_events": ("id",),
    "webhook_resource_responses": ("id",),
}
CONTROL_TABLES = {"sources", "sync_runs", "sync_entity_runs", "sync_state", "sync_cursors",
                  "webhook_events", "webhook_resource_responses", "document_change_log"}
DOCUMENT_CHILDREN = ("document_details", "document_references", "document_sellers")
RAW_DATA_TABLES = set(EXPECTED_PKS) - CONTROL_TABLES
ALLOWED_FK_TARGETS = {
    ("bsale.companies", ("company_id",)),
    ("bsale_raw.sync_runs", ("id",)),
    ("bsale_raw.webhook_events", ("id",)),
}
ACTIVE_WEBHOOK_STATUSES = {"PENDING", "RETRY", "PROCESSING"}


def _sql(path) -> str:
    return strip_comments(path.read_text(encoding="utf-8"))


def _statements(path) -> list[str]:
    body = re.sub(r"\$verify\$.*?\$verify\$", "", _sql(path), flags=re.DOTALL)
    body = re.sub(r"'(?:[^']|'')*'", "''", body)
    return [s.strip() for s in body.split(";") if s.strip()]


# --- estructura ---

def test_ordered_migration_files():
    names = [p.name for p in migration_files()]
    assert names == [
        "001_schema_sources.sql", "002_sync_control.sql", "003_configuration.sql", "004_catalog.sql",
        "005_inventory_pricing.sql", "006_documents.sql", "007_stock_movements.sql", "008_webhooks.sql",
        "009_seed_sources.sql", "010_variant_prices_missing_since.sql",
    ]


def test_all_27_tables_exist_once():
    # 26 de la propuesta + document_change_log (auditoría OC 33, reglas 2.B)
    assert set(TABLES) == set(EXPECTED_PKS)
    assert len(TABLES) == 27
    created = [m for p in migration_files() for m in re.findall(r"CREATE TABLE IF NOT EXISTS bsale_raw\.(\w+)", _sql(p))]
    assert len(created) == len(set(created)) == 27


def test_registry_tables_have_migrations():
    assert {s.raw_table.removeprefix("bsale_raw.") for s in REGISTRY.all()} <= set(TABLES)


def test_parser_reads_every_column_line():
    added: dict[str, int] = {}
    for path in migration_files():
        for match in re.finditer(r"ALTER TABLE bsale_raw\.(\w+) ADD COLUMN", _sql(path)):
            added[match.group(1)] = added.get(match.group(1), 0) + 1
    for path in migration_files():
        for match in re.finditer(r"CREATE TABLE IF NOT EXISTS bsale_raw\.(\w+) \((.*?)\n\);", _sql(path), re.DOTALL):
            lines = [ln for ln in match.group(2).splitlines() if re.match(r"^    [a-z_]", ln)]
            name = match.group(1)
            assert len(lines) + added.get(name, 0) == len(TABLES[name].columns), name


def test_variant_prices_missing_since_added_by_010_only():
    col = TABLES["variant_prices"].columns["missing_since"]
    assert col.type == "timestamp with time zone" and not col.not_null
    created = _sql(next(p for p in migration_files() if p.name == "005_inventory_pricing.sql"))
    block = re.search(r"CREATE TABLE IF NOT EXISTS bsale_raw\.variant_prices \((.*?)\n\);", created, re.DOTALL)
    assert "missing_since" not in block.group(1)
    alter = _statements(next(p for p in migration_files() if p.name == "010_variant_prices_missing_since.sql"))
    assert "ALTER TABLE bsale_raw.variant_prices ADD COLUMN missing_since TIMESTAMPTZ" in alter
    assert not any("DEFAULT" in s.upper() or "NOT NULL" in s.upper() for s in alter if s.startswith("ALTER"))


@pytest.mark.parametrize("table", sorted(EXPECTED_PKS))
def test_primary_keys(table):
    assert TABLES[table].pk == EXPECTED_PKS[table]
    for col in TABLES[table].pk:
        assert TABLES[table].columns[col].not_null


def test_company_id_is_bigint_with_fk_everywhere():
    for name, table in TABLES.items():
        if name == "sync_runs":
            assert "company_id" not in table.columns
            continue
        col = table.columns["company_id"]
        assert col.type == "bigint", name
        assert col.not_null or name == "webhook_events", name
        assert (("company_id",), "bsale.companies", ("company_id",)) in table.fks.values(), name


def test_only_allowed_foreign_keys():
    for name, table in TABLES.items():
        for cols, target, tcols in table.fks.values():
            assert (target, tcols) in ALLOWED_FK_TARGETS, (name, target)


def test_no_secret_columns():
    secret = re.compile(r"token|secret|password|api_key|apikey|authorization|access_key", re.IGNORECASE)
    for name, table in TABLES.items():
        for col in table.columns:
            if (name, col) == ("sources", "token_env"):
                continue
            assert not secret.search(col), (name, col)


def test_no_token_values_in_sql():
    for path in [*migration_files(), VERIFY_FILE]:
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"\b[0-9a-f]{32,}\b", text, re.IGNORECASE), path.name
        assert "access_token" not in text.lower(), path.name


def test_sources_cpn_unique_and_token_env_is_name_only():
    sources = TABLES["sources"]
    assert ("cpn_id",) in sources.uniques.values()
    assert sources.columns["cpn_id"].type == "bigint"
    assert {"company_id", "cpn_id", "name", "token_env", "active", "created_at", "updated_at"} <= set(sources.columns)
    assert "BSALE_TOKEN_" in sources.checks["ck_raw_sources_token_env"]


def test_seed_sources_is_idempotent_and_observed():
    seed = _sql(next(p for p in migration_files() if p.name == "009_seed_sources.sql"))
    assert "ON CONFLICT (company_id) DO NOTHING" in seed
    rows = set(re.findall(r"\((\d+), (\d+), '[^']*', '(BSALE_TOKEN_\w+)'", seed))
    assert rows == {("1", "96674", "BSALE_TOKEN_Mini"), ("2", "5807", "BSALE_TOKEN_Romero"),
                    ("3", "21884", "BSALE_TOKEN_SPA")}


# --- RAW / frescura ---

@pytest.mark.parametrize("table", sorted(RAW_DATA_TABLES))
def test_raw_tables_have_payload_and_freshness(table):
    cols = TABLES[table].columns
    assert cols["payload"].type == "jsonb" and cols["payload"].not_null
    assert cols["payload_hash"].type == "text" and cols["payload_hash"].not_null
    assert cols["api_fetched_at"].type == "timestamp with time zone" and cols["api_fetched_at"].not_null
    assert {"first_seen_at", "last_seen_at", "last_changed_at", "last_source", "sync_run_id"} <= set(cols)


def test_webhook_payload_is_jsonb():
    assert TABLES["webhook_events"].columns["payload"].type == "jsonb"
    assert TABLES["webhook_resource_responses"].columns["body"].type == "jsonb"


def test_reconcile_indexes_by_scope_and_fetched_at():
    def has(table, cols):
        return any(i.table == table and i.columns == cols for i in INDEXES.values())

    assert has("stocks", ("company_id", "office_id", "api_fetched_at"))
    assert has("variant_prices", ("company_id", "price_list_id", "api_fetched_at"))
    for table in ("products", "variants", "clients", "variant_costs"):
        assert has(table, ("company_id", "api_fetched_at")), table


def test_snapshot_started_at_recorded_per_entity_run():
    assert TABLES["sync_entity_runs"].columns["snapshot_started_at"].type == "timestamp with time zone"
    assert "scope" in TABLES["sync_entity_runs"].columns


def test_source_ids_separate_and_not_unique():
    assert TABLES["stocks"].columns["bsale_stock_id"].type == "bigint"
    assert TABLES["variant_prices"].columns["bsale_detail_id"].type == "bigint"
    for table, col in (("stocks", "bsale_stock_id"), ("variant_prices", "bsale_detail_id")):
        assert col not in TABLES[table].pk
        assert all(col not in cols for cols in TABLES[table].uniques.values())
        indexes = [i for i in INDEXES.values() if i.table == table and col in i.columns]
        assert indexes and not any(i.unique for i in indexes)
        assert indexes[0].columns == ("company_id", col)


# --- documentos / open document watch ---

def test_documents_columns_for_open_watch():
    cols = TABLES["documents"].columns
    for col in ("company_id", "document_type_id", "office_id", "state", "commercial_state", "emission_date",
                "generation_date", "api_fetched_at", "payload_hash", "details_complete"):
        assert col in cols, col
    assert cols["generation_date"].type == "timestamp with time zone"
    assert cols["emission_date"].type == "date"


def test_documents_watch_index():
    watch = INDEXES["ix_raw_documents_watch"]
    assert watch.table == "documents"
    assert watch.columns == ("company_id", "document_type_id", "api_fetched_at")
    assert watch.where.strip() == "watch_closed_at IS NULL"
    assert "state" not in watch.where
    assert INDEXES["ix_raw_documents_type_emission"].columns == ("company_id", "document_type_id", "emission_date")


def test_documents_support_versioning_and_grace_watch():
    cols = TABLES["documents"].columns
    for col in ("payload_hash", "first_seen_at", "last_seen_at", "last_changed_at", "api_fetched_at",
                "version_hash", "children_hash", "version_changed_at", "attributes_payload",
                "watch_terminal_seen_at", "watch_stable_reads", "watch_closed_at"):
        assert col in cols, col
    assert cols["attributes_payload"].type == "jsonb"
    assert cols["watch_stable_reads"].not_null


@pytest.mark.parametrize("table", DOCUMENT_CHILDREN)
def test_document_children_bound_to_parent_and_version(table):
    cols = TABLES[table].columns
    assert cols["company_id"].not_null and cols["document_id"].not_null
    assert cols["document_version_hash"].type == "text" and cols["document_version_hash"].not_null


def test_document_change_log_supports_audit_and_affected_variants():
    log = TABLES["document_change_log"]
    for col in ("company_id", "document_id", "detected_at", "detected_by", "sync_run_id", "webhook_event_id",
                "previous_version_hash", "version_hash", "previous_state", "state", "details_changed",
                "references_changed", "sellers_changed", "attributes_changed"):
        assert col in log.columns, col
    for col in ("previous_variant_ids", "current_variant_ids", "affected_variant_ids"):
        assert log.columns[col].type == "bigint[]" and log.columns[col].not_null
    assert "payload" not in log.columns
    assert not any(target != "bsale.companies" for _, target, _ in log.fks.values())
    assert INDEXES["ix_raw_document_change_log_document"].columns == ("company_id", "document_id", "detected_at")
    assert INDEXES["ix_raw_document_change_log_pending_stock"].where.strip() == "stock_refresh_done_at IS NULL"


def test_no_terminal_state_semantics_in_sql():
    for name in ("documents",):
        assert not any("state" in expr for expr in TABLES[name].checks.values() if "last_source" not in expr)


CHILD_PARENT_COLUMN = {
    "document_details": "document_id",
    "document_references": "document_id",
    "document_sellers": "document_id",
    "stock_reception_details": "reception_id",
    "stock_consumption_details": "consumption_id",
}


def test_child_pk_scoped_by_parent():
    """El id del hijo nunca identifica solo a su padre: la PK siempre empieza por (company_id, parent_id)."""
    for table, parent in CHILD_PARENT_COLUMN.items():
        assert TABLES[table].columns[parent].not_null, table
        assert TABLES[table].pk[:2] == ("company_id", parent), table


def test_child_own_id_indexed_but_not_unique():
    for table in ("document_details", "document_references", "stock_reception_details", "stock_consumption_details"):
        matches = [i for i in INDEXES.values() if i.table == table and i.columns == ("company_id", "bsale_id")]
        assert matches and not any(i.unique for i in matches), table
        assert not TABLES[table].uniques, table


def test_document_children_indexed_by_document_for_atomic_replace():
    for table in DOCUMENT_CHILDREN:
        assert TABLES[table].pk[:2] == ("company_id", "document_id"), table


# --- webhooks ---

def test_webhook_dedupe_not_permanently_unique():
    events = TABLES["webhook_events"]
    assert not events.uniques
    for ix in INDEXES.values():
        if ix.table != "webhook_events" or not ix.unique:
            continue
        assert ix.columns == ("refresh_key",)
        assert ix.where, ix.name
        statuses = set(re.findall(r"'([A-Z_]+)'", ix.where))
        assert statuses and statuses <= ACTIVE_WEBHOOK_STATUSES, ix.name
    assert not INDEXES["ix_raw_webhook_events_dedupe"].unique
    assert "coalesced_into_id" in events.columns


# --- scope ---

def test_scope_columns_without_check():
    for table in ("sync_state", "sync_cursors", "sync_entity_runs"):
        assert TABLES[table].columns["scope"].type == "text"
        assert not any("scope" in expr for expr in TABLES[table].checks.values())
    assert "scope" in TABLES["sync_state"].pk and "scope" in TABLES["sync_cursors"].pk


# --- enums ---

def test_check_enums_match_python():
    expected = {
        "last_source": {m.value for m in SyncMode},
        "mode": {m.value for m in SyncMode},
        "envelope": {e.value for e in ResponseEnvelope},
        "detected_by": {m.value for m in SyncMode},
        "change_kind": {k.value for k in DocumentChangeKind},
    }
    seen = set()
    for name, table in TABLES.items():
        for expr in table.checks.values():
            parsed = check_in_values(expr)
            if parsed is None:
                continue
            column, values = parsed
            seen.add(column)
            if column == "status":
                target = {s.value for s in WebhookStatus} if name == "webhook_events" else {s.value for s in RunStatus}
                assert values == target, name
            else:
                assert values == expected[column], (name, column)
    assert seen == {"last_source", "mode", "status", "envelope", "detected_by", "change_kind"}


# --- seguridad de los archivos SQL ---

FORBIDDEN_STATEMENT = re.compile(r"^(DROP|TRUNCATE|DELETE|UPDATE|ALTER|GRANT|REVOKE|COPY|VACUUM)\b", re.IGNORECASE)
# Única forma de ALTER permitida: agregar UNA columna (no destructivo); nunca DROP / TYPE / RENAME.
ADD_COLUMN_STATEMENT = re.compile(r"^ALTER TABLE bsale_raw\.[a-z_]+ ADD COLUMN [a-z_][a-z0-9_]* [A-Z]+(\[\])?( NOT NULL)?$")


@pytest.mark.parametrize("path", migration_files(), ids=lambda p: p.name)
def test_migrations_are_transactional_and_non_destructive(path):
    statements = _statements(path)
    assert statements[0].upper() == "BEGIN" and statements[-1].upper() == "COMMIT"
    for stmt in statements:
        if ADD_COLUMN_STATEMENT.match(stmt):
            continue
        assert not FORBIDDEN_STATEMENT.match(stmt), stmt[:60]
        assert "CONCURRENTLY" not in stmt.upper()
        if stmt.upper().startswith("INSERT"):
            assert stmt.startswith("INSERT INTO bsale_raw."), stmt[:60]
        allowed_start = ("BEGIN", "COMMIT", "CREATE SCHEMA IF NOT EXISTS bsale_raw", "CREATE TABLE IF NOT EXISTS bsale_raw.",
                         "CREATE INDEX IF NOT EXISTS", "CREATE UNIQUE INDEX IF NOT EXISTS", "COMMENT ON",
                         "INSERT INTO bsale_raw.")
        assert stmt.startswith(allowed_start), stmt[:60]


@pytest.mark.parametrize("path", [*migration_files(), VERIFY_FILE], ids=lambda p: p.name)
def test_sql_punctuation_balanced(path):
    body = re.sub(r"\$verify\$.*?\$verify\$", "", _sql(path), flags=re.DOTALL) if path != VERIFY_FILE else _sql(path)
    assert re.sub(r"'(?:[^']|'')*'", "", body).count("'") == 0, "comilla simple sin cerrar"
    for stmt in _statements(path):
        assert stmt.count("(") == stmt.count(")"), stmt[:60]


@pytest.mark.parametrize("path", migration_files(), ids=lambda p: p.name)
def test_create_table_items_separated_by_commas(path):
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\S+) \((.*?)\n\);", _sql(path), re.DOTALL):
        items: list[str] = []
        for line in m.group(2).strip("\n").splitlines():
            if not line.strip():
                continue
            if line.startswith("        ") and items:
                items[-1] += " " + line.strip()
            else:
                items.append(line.strip())
        assert all(item.endswith(",") for item in items[:-1]), (m.group(1), [i for i in items[:-1] if not i.endswith(",")])
        assert not items[-1].endswith(","), m.group(1)


@pytest.mark.parametrize("path", [*migration_files(), VERIFY_FILE], ids=lambda p: p.name)
def test_no_sql_against_legacy_objects(path):
    sql = re.sub(r"'(?:[^']|'')*'", "''", _sql(path))
    for match in re.finditer(r"\b(bsale|distribuidora|public|app|analytics)\.(\w+)", sql):
        assert f"{match.group(1)}.{match.group(2)}" == "bsale.companies", match.group(0)
        if path != VERIFY_FILE:
            assert sql[max(0, match.start() - 11):match.start()] == "REFERENCES ", match.group(0)


def test_verify_is_read_only():
    body = _sql(VERIFY_FILE)
    assert body.strip().startswith("DO $verify$")
    inner = re.sub(r"'(?:[^']|'')*'", "''", body)
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT)\b", inner, re.IGNORECASE)


def test_verify_block_matches_migrations():
    assert verify_expected_block() == render_expected_block()
