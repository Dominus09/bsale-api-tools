# Inteligencia de Bodega — Slice 1.1 (implementación)

**Fecha:** 2026-11-24  
**Alcance:** schema + API + seed BC draft + tests PG aislado  
**No:** frontend, Konva, commit/push, SQL productivo, PWA, Coolify

---

## 1. Resumen

Se implementó el backend del layout físico en schema `warehouse`, con:

- Identidad permanente (`physical_elements`) separada de geometría versionada (`layout_elements`).
- Revisiones `draft` / `published` / `archived` (escritura solo draft; un draft por piso).
- API `/warehouse/*` bajo `require_management_access`.
- Seed idempotente Bodega Central (office Bsale 1) en borrador, sin posiciones individuales ni inventario.
- 15 pruebas OK en Postgres temporal (`pgserver`).

## 2. Archivos creados / modificados

### Creados
- `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql`
- `backend/sql/warehouse/002_seed_bodega_central_draft_AWAITING_APPROVAL.sql`
- `backend/services/warehouse/__init__.py`
- `backend/services/warehouse/coordinates.py`
- `backend/services/warehouse/errors.py`
- `backend/services/warehouse/repository.py`
- `backend/services/warehouse/service.py`
- `backend/routers/warehouse.py`
- `backend/tests/test_warehouse_layout_isolated_pg.py`
- `backend/tests/test_warehouse_layout_unit.py`
- `backend/tests/_run_warehouse_isolated.py`
- `_audit_artifacts/inteligencia_bodega_slice_1_1.md` (este informe)

### Modificados
- `backend/main.py` — `include_router(warehouse.router)`

## 3. Modelo de datos definitivo (slice 1.1)

| Tabla | Rol |
|-------|-----|
| `warehouse.facilities` | Instalación (BC ↔ `bsale_office_id=1`) |
| `warehouse.floors` | Piso de edificio (PISO1); dims length/width metros |
| `warehouse.physical_elements` | **Identidad permanente** (code, type, parent, active/retired) |
| `warehouse.layout_revisions` | draft \| published \| archived + `row_version` |
| `warehouse.layout_elements` | **Geometría métrica** por revisión ↔ physical_id |
| `warehouse.layout_audit` | Auditoría de acciones |

## 4. Diagrama de relaciones

```
facilities 1──* floors
floors 1──* layout_revisions
floors 1──* physical_elements
physical_elements.parent_id → physical_elements (mismo floor; sin ciclos)
layout_revisions 1──* layout_elements
layout_elements.physical_element_id → physical_elements
layout_revisions 1──* layout_audit
```

Futuros pallets/ocupación deberán FK a `physical_elements.id` (no solo a `layout_elements.id`).

## 5. Identidad permanente vs geometría versionada

| | Permanente | Versionada |
|--|------------|------------|
| Tabla | `physical_elements` | `layout_elements` |
| Qué guarda | code, name, type, jerarquía, status | x/y/width/depth/rotación, validation_status |
| Al corregir coords/nombre | Misma `id` | Nueva fila o upsert en el draft |
| Al publicar (futuro) | Inmutable id | Snapshot geométrico en revisión published |
| Retiro | `status=retired` (código liberable para **nuevo** id) | Se elimina del draft; historial audit + id físico permanece |

## 6. Endpoints

| Método | Ruta |
|--------|------|
| GET | `/warehouse/meta/coordinate-convention` |
| GET | `/warehouse/facilities` |
| GET | `/warehouse/facilities/{id}/floors` |
| GET | `/warehouse/floors/{id}/revisions` |
| POST | `/warehouse/floors/{id}/draft` |
| GET | `/warehouse/revisions/{id}` |
| POST | `/warehouse/revisions/{id}/elements` |
| POST | `/warehouse/revisions/{id}/elements/batch` |
| GET | `/warehouse/revisions/{id}/audit` |

**Batch:** `upserts` + `retire_physical_ids` + `remove_geometry_physical_ids` + `expected_row_version`.  
Ausentes en el payload **no** se borran. Transacción atómica. 409 en conflicto de versión / revisión no draft.

## 7. Permisos

- Dependencia: `require_management_access` (`backend/auth/permissions.py`).
- Acceso efectivo: roles `gerencia`, `adm`, `admin` (+ OWNER / flags management).
- Exige app ERP vía `require_erp_app`.
- **No** se modificó `app.user_erp_capabilities`.
- Perfil PWA `bodega` **no** concede acceso.
- Futuro documentado: `warehouse_layout_view|edit|publish`.

## 8. SQL para revisión

Ambos archivos llevan sufijo **`AWAITING_APPROVAL`**. No ejecutados en producción.

Orden:
1. `001_schema_layout_AWAITING_APPROVAL.sql`
2. `002_seed_bodega_central_draft_AWAITING_APPROVAL.sql`

## 9. Pruebas ejecutadas

```
python -m backend.tests._run_warehouse_isolated
→ 15 passed (PG temporal data_directory bajo Temp\; DSN sin analytics/quillotana)
```

Cobertura: seed idempotente, identidad estable, batch sin delete implícito, concurrencia, parent inválido, retire+reuso de código, published no escribible, sin dependencia de schemas Bsale, auditoría, 403 management, convención métrica, OpenAPI routes.

## 10. Seed Bodega Central (datos)

- Facility `BC` / Bodega Central / office **1**
- Floor `PISO1` 23,10 × 13,70 m; portón 4,35 m (meta)
- Elementos: `BC-BND`, `PORTON`, `IZ`, `CIRC`, `DER`, `OFC`, `P1`…`P7`, `ESC`
- **0** posiciones de almacenamiento individuales
- Pallet estándar 1,26×1,06 en `facilities.meta_json`
- Catálogo de simbología en meta + `coordinates.SYMBOLOGY_CATALOG` (UI en 1.2)

## 11. Coordenadas / dims pendientes de validación

Todas las geometrías del seed: `validation_status=pending_validation`, `meta.provisional=true`.

| Elemento | Nota |
|----------|------|
| IZ altura 4,00 m | Referencial; **no** máximo seguro |
| CIRC ancho 4,70 m | Mid del rango 4,60–4,80 |
| DER overhang 0,60 m | Observado; **no** autorizado |
| OFC / ESC / PORTON posición | Placeholders |
| Pasillos P1–P7 | 2,80×6,00 referenciales; sin posiciones |
| Remanente norte ~0,35 m | Diferencia layout vs 13,70 — no forzada |

Convención: origen SW; +X largo; +Y ancho; rotación ° CCW centro rect; `elevation_m` ≠ `stack_levels_max`.

## 12. Git

- HEAD previo: `517ceb5` (sin commit de este slice — **autorización pendiente**).
- Diff local: archivos warehouse listados en §2 (working tree).
- Sin push / redeploy.

## 13. Riesgos / bloqueos

- SQL prod no aplicado (esperado).
- Publicación formal y validación geométrica completa → slices posteriores.
- Konva/compatibilidad React 19 → verificar en 1.2 antes de instalar.
- Menú ERP `/inteligencia-bodega` aún no existe (frontend fuera de 1.1).

## 14. Propuesta slice 1.2

1. Verificar compatibilidad `konva` + `react-konva` con Next 16 / React 19.  
2. Página `/inteligencia-bodega` + editor 2D (toolbar / canvas / properties).  
3. Botón **Simbología** (3 pestañas) + tooltips desde `SYMBOLOGY_CATALOG`.  
4. Consumir GET revision + batch save con `expected_row_version`.  
5. Generador de posiciones (UI) + validaciones geométricas básicas.  
6. Entrada de menú sidebar (sin tocar PWA/Choferes/Vendedores).

**STOP.** Esperar autorización expresa para 1.2 y para commit/push/migración.
