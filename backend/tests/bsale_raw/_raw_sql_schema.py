"""Parser estático de las migraciones ``backend/sql/bsale_raw`` (sólo tests; no conecta a la BD).

Convención de formato que el parser asume (y los tests exigen):
- una columna por línea: ``    nombre TIPO [NOT NULL] [DEFAULT ...],``;
- PK / FK / UNIQUE / CHECK siempre como ``CONSTRAINT <nombre> ...``;
- índices con ``CREATE [UNIQUE] INDEX IF NOT EXISTS <nombre> ON bsale_raw.<tabla> (...)``;
- migraciones posteriores: sólo ``ALTER TABLE bsale_raw.<tabla> ADD COLUMN <nombre> TIPO [NOT NULL];``
  (una columna por sentencia, sin ``IF NOT EXISTS``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "bsale_raw"
VERIFY_FILE = SQL_DIR / "verify_bsale_raw.sql"

SQL_TYPES = {
    "BIGSERIAL": "bigint",
    "BIGINT": "bigint",
    "TEXT": "text",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "NUMERIC": "numeric",
    "JSONB": "jsonb",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMPTZ": "timestamp with time zone",
}

_TABLE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS bsale_raw\.(\w+) \((.*?)\n\);", re.DOTALL)
_COLUMN_RE = re.compile(
    rf"^\s+([a-z_][a-z0-9_]*)\s+({'|'.join(SQL_TYPES)})(\[\])?(?=[\s,]|$)(.*)$", re.MULTILINE
)
_PK_RE = re.compile(r"CONSTRAINT (\w+) PRIMARY KEY \(([^)]*)\)")
_UNIQUE_RE = re.compile(r"CONSTRAINT (\w+) UNIQUE \(([^)]*)\)")
_FK_RE = re.compile(r"CONSTRAINT (\w+)\s+FOREIGN KEY \(([^)]*)\) REFERENCES ([\w.]+) \(([^)]*)\)")
_CHECK_RE = re.compile(r"CONSTRAINT (\w+)\s+CHECK \((.*?)\)\s*(?:,|$)", re.DOTALL)
_CHECK_IN_RE = re.compile(r"^(\w+) IN \(([^)]*)\)$")
ADD_COLUMN_RE = re.compile(
    rf"ALTER TABLE bsale_raw\.(\w+) ADD COLUMN ([a-z_][a-z0-9_]*) ({'|'.join(SQL_TYPES)})(\[\])?([^;]*);"
)
_INDEX_RE = re.compile(
    r"CREATE (UNIQUE )?INDEX IF NOT EXISTS (\w+)\s+ON bsale_raw\.(\w+) \(([^)]*)\)(?:\s+WHERE ([^;]*))?;"
)


@dataclass
class Column:
    name: str
    type: str
    not_null: bool


@dataclass
class Table:
    name: str
    file: str
    columns: dict[str, Column] = field(default_factory=dict)
    pk: tuple[str, ...] = ()
    pk_name: str = ""
    uniques: dict[str, tuple[str, ...]] = field(default_factory=dict)
    fks: dict[str, tuple[tuple[str, ...], str, tuple[str, ...]]] = field(default_factory=dict)
    checks: dict[str, str] = field(default_factory=dict)


@dataclass
class Index:
    name: str
    table: str
    columns: tuple[str, ...]
    unique: bool
    where: str | None


def _cols(text: str) -> tuple[str, ...]:
    return tuple(c.strip().split()[0] for c in text.split(",") if c.strip())


def strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def migration_files() -> list[Path]:
    return sorted(SQL_DIR.glob("[0-9][0-9][0-9]_*.sql"))


def parse() -> tuple[dict[str, Table], dict[str, Index]]:
    tables: dict[str, Table] = {}
    indexes: dict[str, Index] = {}
    for path in migration_files():
        sql = strip_comments(path.read_text(encoding="utf-8"))
        for match in _TABLE_RE.finditer(sql):
            name, body = match.group(1), match.group(2)
            table = Table(name=name, file=path.name)
            for col in _COLUMN_RE.finditer(body):
                col_type = SQL_TYPES[col.group(2)] + (col.group(3) or "")
                table.columns[col.group(1)] = Column(col.group(1), col_type, "NOT NULL" in col.group(4))
            if pk := _PK_RE.search(body):
                table.pk_name, table.pk = pk.group(1), _cols(pk.group(2))
            for uq in _UNIQUE_RE.finditer(body):
                table.uniques[uq.group(1)] = _cols(uq.group(2))
            for fk in _FK_RE.finditer(body):
                table.fks[fk.group(1)] = (_cols(fk.group(2)), fk.group(3), _cols(fk.group(4)))
            for ck in _CHECK_RE.finditer(body):
                table.checks[ck.group(1)] = " ".join(ck.group(2).split())
            tables[name] = table
        for add in ADD_COLUMN_RE.finditer(sql):
            col_type = SQL_TYPES[add.group(3)] + (add.group(4) or "")
            tables[add.group(1)].columns[add.group(2)] = Column(add.group(2), col_type, "NOT NULL" in add.group(5))
        for ix in _INDEX_RE.finditer(sql):
            indexes[ix.group(2)] = Index(ix.group(2), ix.group(3), _cols(ix.group(4)), bool(ix.group(1)), ix.group(5))
    return tables, indexes


def check_in_values(expr: str) -> tuple[str, set[str]] | None:
    match = _CHECK_IN_RE.match(expr)
    if not match:
        return None
    return match.group(1), set(re.findall(r"'([^']+)'", match.group(2)))


def expected_catalog() -> dict[str, list[str]]:
    """Listas que ``verify_bsale_raw.sql`` compara contra el catálogo de PostgreSQL."""
    tables, indexes = parse()
    out: dict[str, list[str]] = {
        "expected_tables": [], "expected_columns": [], "expected_constraints": [],
        "expected_pks": [], "expected_fks": [], "expected_indexes": [],
    }
    for t in sorted(tables.values(), key=lambda t: t.name):
        out["expected_tables"].append(t.name)
        for c in t.columns.values():
            out["expected_columns"].append(f"{t.name}.{c.name}:{c.type}:{'NOT NULL' if c.not_null else 'NULL'}")
        out["expected_constraints"].append(f"{t.name}.{t.pk_name}:p")
        out["expected_pks"].append(f"{t.name}:{','.join(t.pk)}")
        out["expected_indexes"].append(f"{t.name}.{t.pk_name}:u")
        for name in t.uniques:
            out["expected_constraints"].append(f"{t.name}.{name}:u")
            out["expected_indexes"].append(f"{t.name}.{name}:u")
        for name, (cols, target, tcols) in t.fks.items():
            out["expected_constraints"].append(f"{t.name}.{name}:f")
            out["expected_fks"].append(f"{t.name}.{name}:{','.join(cols)}->{target}({','.join(tcols)})")
        for name in t.checks:
            out["expected_constraints"].append(f"{t.name}.{name}:c")
    for ix in sorted(indexes.values(), key=lambda i: (i.table, i.name)):
        out["expected_indexes"].append(f"{ix.table}.{ix.name}:{'u' if ix.unique else 'i'}")
    return {k: sorted(v) for k, v in out.items()}


def render_expected_block() -> str:
    lines = []
    for name, values in expected_catalog().items():
        lines.append(f"    {name} text[] := ARRAY[")
        lines.extend(f"        '{v}'," for v in values[:-1])
        lines.append(f"        '{values[-1]}'")
        lines.append("    ];")
    return "\n".join(lines)


def write_expected_block() -> None:
    """Regenera el bloque GENERATED de verify_bsale_raw.sql desde las migraciones."""
    text = VERIFY_FILE.read_text(encoding="utf-8")
    text = re.sub(
        r"(-- BEGIN GENERATED EXPECTED\n).*?(\n\s*-- END GENERATED EXPECTED)",
        lambda m: m.group(1) + render_expected_block() + m.group(2),
        text,
        flags=re.DOTALL,
    )
    VERIFY_FILE.write_text(text, encoding="utf-8", newline="\n")


def verify_expected_block() -> str:
    text = VERIFY_FILE.read_text(encoding="utf-8")
    match = re.search(r"-- BEGIN GENERATED EXPECTED\n(.*?)\n\s*-- END GENERATED EXPECTED", text, re.DOTALL)
    assert match, "bloque generado no encontrado en verify_bsale_raw.sql"
    return match.group(1)
