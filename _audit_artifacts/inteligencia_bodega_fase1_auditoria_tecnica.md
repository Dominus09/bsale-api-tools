# Inteligencia de Bodega — Auditoría técnica (solo lectura) + plan Fase 1

**Fecha:** 2026-11-24  
**Repo:** `bsale-api-tools` · **HEAD:** `517ceb5` (sync `origin/main`)  
**Alcance:** auditoría + arquitectura propuesta. **Sin implementación. Sin SQL prod. Sin push adicional.**

---

## 0. Estado Git (inspección)

| Ítem | Hallazgo |
|------|----------|
| Rama | `main` = `origin/main` @ `517ceb5` |
| Cambios fuente pendientes | Ninguno relevante (solo `__pycache__`, `offline-build.json`, artefactos `_tmp_*` / `_audit_*` no relacionados) |
| Worktrees | Existen ramas lab/auth/costos en otras carpetas; no tocar |
| Restricción | No modificar esos cambios locales ajenos |

---

## 1. Hallazgos de auditoría (archivos reales)

### 1.1 Frontend ERP

| Hallazgo | Evidencia |
|----------|-----------|
| App Next.js 16 + React 19, App Router | `frontend/package.json`, `frontend/app/(dashboard)/` |
| Shell: Sidebar + Header + auth gate | `frontend/app/(dashboard)/layout.tsx`, `frontend/components/layout/sidebar.tsx` |
| Navegación por secciones (Logística, Sucursales, Distribuidora, Operaciones…) | `sidebar.tsx` `navSections` |
| Placeholder “Stock” deshabilitado | `sidebar.tsx` ~L181: `{ href: "#", label: "Stock", icon: Warehouse, disabled: true }` |
| UI kit Radix + Tailwind | `frontend/components/ui/*` |
| Permisos UI | `frontend/lib/permissions.ts` + `useAuthMe` |
| Mapas geo (Leaflet), no editor de plano métrico | deps `leaflet`, `react-leaflet`; uso en planif./rutero |
| DnD listas | `@dnd-kit/*` (útil patrones UX, no canvas de bodega) |
| Sin Konva / Fabric / Three / Pixi | `frontend/package.json` |
| Página logística existente (no WMS) | `/distribuidora/maestro-logistico` |

### 1.2 PWA multimódulo

| Hallazgo | Evidencia |
|----------|-----------|
| Módulo `bodega` ya reservado, status `soon` | `apps/pwa/lib/modules.ts` |
| Placeholder `/bodega` | `apps/pwa/app/(app)/bodega/page.tsx` |
| Perfil operativo `bodega` | `backend/sql/065_auth_user_operational_profiles.sql` |
| **Fase 1: no tocar PWA** (regla del brief) | Aislamiento natural |

### 1.3 Backend FastAPI

| Hallazgo | Evidencia |
|----------|-----------|
| Montaje central | `backend/main.py` (`include_router` ~L118–192) |
| Dominios: auth, catalog, analytics_v2, gondolas, distribuidora*, operaciones, diagnostics | mismos routers |
| Patrón módulos aislados | p.ej. `backend/services/gondolas/`, `backend/routers/gondolas.py` prefix `/gondolas` |
| Auth staff JWT + management | `backend/auth/permissions.py` `require_management_access` |
| Capacidades ERP extensibles | `app.user_erp_capabilities` (`069_user_erp_capabilities.sql`; hoy solo `simple_planning`) |
| Multiapp / perfiles PWA | `backend/auth/multiapp.py` |

### 1.4 PostgreSQL / migraciones

| Hallazgo | Evidencia |
|----------|-----------|
| Schemas vivos | `bsale`, `bsale_raw`, `distribuidora`, `app`, `analytics`, `public` |
| **No existen** tablas warehouse/bodega/pallet/layout/ubicacion WMS | consulta `information_schema` → vacío |
| Oficinas Bsale (company 3) | `bsale.offices`: **1 = BODEGA CENTRAL**, 2 BODEGA 1, 3 SUPERMERCADO… |
| Numeración SQL fragmentada | `backend/sql/` hasta **069**; `backend/sql/distribuidora/` hasta **062** AWAITING; `gondolas/001–002`; `analytics_v2/001`; `bsale_raw/001–012` |
| Convención aditiva + AWAITING_APPROVAL | p.ej. `062_*_AWAITING_APPROVAL.sql`, gondolas “NO ejecutar sin autorización” |
| PostGIS | No usado como SoT de layouts (geometrías en planif. son GeoJSON de rutas ORS, no bodega) |

### 1.5 Maestros logísticos reutilizables (Fase 2+, no Fase 1)

| Recurso | Uso futuro |
|---------|------------|
| `bsale.products_master` (UPB, peso, cm) | Contenido de pallets / cubicación |
| `bsale_raw.variants/stocks` | Stock comercial (solo lectura; **no escribir**) |
| `bsale.offices` | Vincular instalación física ↔ oficina Bsale (office_id=1) |

### 1.6 Pruebas

| Patrón | Evidencia |
|--------|-----------|
| pytest unit + HTTP TestClient | `backend/tests/test_*.py` |
| PG aislado para migraciones sensibles | `*_isolated_pg.py`, `SIMPLE_PLANNING_TEST_DSN` |
| Frontend: lint/build Next | `frontend/package.json` scripts |

### 1.7 Riesgos de interferencia

1. Confundir `bsale.offices` / stock Bsale con posiciones físicas WMS.  
2. Activar PWA módulo `bodega` prematuramente.  
3. Colisión de numeración SQL si se usa `070` vs `distribuidora/063` sin namespace.  
4. Leaflet como “editor” (inadecuado para metros interiores).  
5. Refactors en `sidebar`/auth que afecten Choferes/Vendedores.  
6. Escribir en `bsale_raw` o alterar syncs.

---

## 2. Arquitectura propuesta del módulo (Fase 1)

```
ERP (Next)                         FastAPI                         PostgreSQL
─────────────────                  ─────────                       ──────────
/inteligencia-bodega               /warehouse/*  (nuevo)           schema warehouse
  ├─ (hub mínimo: solo layout)       routers/warehouse.py            (nuevo, aislado)
  └─ /editor                         services/warehouse/             ≠ bsale_raw
       Gemelo 2D + propiedades       repositories/warehouse/         ≠ stock comercial
```

- **Aislamiento:** schema `warehouse` (recomendado) o `app.warehouse_*` si se prefiere no crear schema; preferencia: **`warehouse`** para no mezclar con góndolas/auth.  
- **Vínculo opcional:** `warehouse.facilities.bsale_office_id` → `bsale.offices.bsale_id` (FK lógica, sin cascade destructivo).  
- **No** escribe stock Bsale. **No** toca PWA en Fase 1.

---

## 3. Modelo de datos preliminar (Fase 1 — layout físico)

> Nombres propuestos (aún no creados). Preparados para pallets/contenido en Fase 2 sin romper IDs.

```
warehouse.facilities          -- instalación (Bodega Central BC)
warehouse.floors              -- piso edificio (Piso 1 ≠ nivel pallet)
warehouse.layout_revisions    -- draft | published | archived + optimistic lock
warehouse.layout_elements     -- geometría métrica + tipo + code visible
warehouse.layout_audit        -- quién/cuándo create|update|publish
```

### `layout_elements` (núcleo geométrico)

| Campo | Notas |
|-------|-------|
| `id` UUID/BIGSERIAL | ID interno persistente |
| `revision_id` | FK revisión |
| `element_type` | facility_boundary, sector, aisle, position, wall, pillar, door, gate, stair, office, circulation, obstacle, unusable… |
| `code` | visible corto (`IZ-08`, `P2-03`) |
| `name` | etiqueta |
| `parent_element_id` | jerarquía flexible (sector→pasillo→posición) |
| `x_m`, `y_m` | origen local metros (plano) |
| `z_m` / `elevation_m` | ref. vertical edificio (no stack pallet) |
| `width_m`, `depth_m`, `height_clear_m` | dimensiones reales |
| `rotation_deg` | |
| `geom_type` | `rect` ahora; `polygon` JSONB futuro |
| `polygon_m` JSONB nullable | vértices metros |
| `stack_levels_max` nullable | futuro apilamiento; **no capacidad autorizada** |
| `constraints_json` | restricciones / `validation_status` |
| `validation_status` | `draft` \| `pending_validation` \| `validated` |
| `meta_json` | observaciones, prefijos, generador |

### Versionado

- Edición solo en revisión `draft`.  
- `publish` clona/congela snapshot → `published`; draft anterior puede archivarse.  
- `row_version` / `updated_at` + `If-Match` o `expected_updated_at` para concurrencia.

### Relación (diagrama textual)

```
facilities 1──* floors
floors 1──* layout_revisions
layout_revisions 1──* layout_elements
layout_elements (parent_element_id) ──┐
                                      └── self (árbol flexible)
layout_revisions 1──* layout_audit
```

**Futuro (no crear en Fase 1):** `pallets`, `pallet_contents`, `location_occupancy` referenciando `layout_elements.id` de tipo `position`.

---

## 4. Endpoints FastAPI propuestos

Prefijo: `/warehouse` (montado en `main.py` como góndolas).

| Método | Ruta | Rol |
|--------|------|-----|
| GET | `/warehouse/facilities` | listar instalaciones |
| GET | `/warehouse/facilities/{id}/floors` | pisos |
| GET | `/warehouse/floors/{id}/revisions` | listar revisiones |
| GET | `/warehouse/revisions/{id}` | layout completo (elementos) |
| POST | `/warehouse/floors/{id}/revisions` | crear draft |
| PATCH | `/warehouse/revisions/{id}/elements` | upsert batch elementos |
| POST | `/warehouse/revisions/{id}/generate-positions` | generador (dry geometry) |
| POST | `/warehouse/revisions/{id}/validate` | solapes / fuera de sector |
| POST | `/warehouse/revisions/{id}/publish` | draft → published |
| GET | `/warehouse/revisions/{id}/audit` | auditoría |

Auth: JWT staff + **capability** nueva `warehouse_layout_edit` (y/o gerencia). Lectura: `warehouse_layout_view`.

---

## 5. Pantallas y componentes ERP

### Rutas (nuevas)

- `frontend/app/(dashboard)/inteligencia-bodega/page.tsx` — hub mínimo (lista instalaciones / link editor).  
- `frontend/app/(dashboard)/inteligencia-bodega/editor/page.tsx` — Editor Maestro 2D.

### Componentes a crear (desde cero)

```
frontend/components/warehouse/
  WarehouseEditorShell.tsx      -- 3 paneles
  EditorToolbar.tsx
  FloorPlanCanvas.tsx           -- canvas métrico
  PropertiesPanel.tsx
  PositionGeneratorDialog.tsx
  GeometryValidationBanner.tsx
```

### Reutilizar

- `Sidebar` / layout dashboard / `Button` `Input` `Dialog` `Select` `Tabs` `Tooltip` `ScrollArea` `Sheet`.  
- Patrón API client en `frontend/lib/api.ts` (añadir sección warehouse).  
- `hasManagementAccess` / futuro flag capability como `simple_planning`.

### Sidebar

Nueva sección **“Inteligencia de Bodega”** (o ítem bajo Logística). **No** reutilizar el placeholder “Stock” disabled sin renombrar/clarificar (Stock ≠ layout WMS).

---

## 6. Tecnología del editor gráfico — recomendación

| Opción | Veredicto |
|--------|-----------|
| **Leaflet** | Rechazado para editor métrico indoor (CRS geo, no metros locales como SoT) |
| **SVG + React** | Viable para MVP muy chico; débil en pan/zoom/perf con cientos de posiciones |
| **Canvas 2D crudo** | Control total; más código propio |
| **Konva + react-konva** | **Recomendado**: capas, transform, hit-test, zoom/pan, buen rendimiento, mantenible; sin 3D |
| Three.js | Diferir a Fase 7 |

**Decisión propuesta:** añadir `konva` + `react-konva` solo en ERP. SoT = metros en API; Konva solo escala de vista (`px_per_meter`).  
Alternativa zero-dep: Canvas 2D si se quiere evitar dependencia — mayor costo de desarrollo.

---

## 7. Layout inicial Bodega Central (estrategia)

1. Seed **solo en draft** vía migración de datos o endpoint `seed-bc-draft` (autorizado).  
2. Facility `BC`, floor `Piso 1`, boundary 23,10 × 13,70 m, portón 4,35 m.  
3. Sector `IZ` profundidad 2,65 m; circulación central 4,60–4,80 m (rango editable, no forzar un solo valor).  
4. Sector derecho: pasillos `P1`…`P7` como elementos `aisle`/`sector` 2,80 × 6,00 m, altura libre 2,75 m; **sin** generar N fijo de posiciones.  
5. Pallet ref. 1,26 × 1,06 m en config del generador (no inventario).  
6. Todos los elementos con `validation_status=pending_validation` donde haya duda (oficina, escalera, pilares, protrusión 0,60 m).  
7. **No** importar productos del Excel como contenido.

---

## 8. Estrategia de permisos

| Capa | Propuesta |
|------|-----------|
| Backend | Extender CHECK de `app.user_erp_capabilities` con `warehouse_layout_view` / `warehouse_layout_edit` **o** exigir `require_management_access` en Fase 1 MVP |
| UI | Gate menú con management o capability (mismo patrón que `simple_planning`) |
| PWA perfil `bodega` | **No** concede editor ERP automáticamente |
| Oficinas | Opcional: filtrar facilities por `app.user_offices` más adelante |

---

## 9. Estrategia de migraciones

1. Carpeta nueva: `backend/sql/warehouse/001_schema_layout.sql` (+ `002_seed_bodega_central_draft.sql` separado).  
2. Prefijo `AWAITING_APPROVAL` hasta autorización.  
3. **No** reutilizar número `070` del árbol raíz sin documentar; preferir namespace `warehouse/001`.  
4. Idempotente, aditivo, sin tocar `bsale`/`bsale_raw`/`distribuidora`.  
5. Suite pytest con PG aislado (patrón `*_isolated_pg.py`).  
6. **Cero** ejecución en producción en esta auditoría.

---

## 10. Archivos a crear / modificar (cuando se autorice)

### Crear

- `backend/sql/warehouse/001_*.sql`, `002_seed_*.sql`  
- `backend/routers/warehouse.py`  
- `backend/services/warehouse/*`  
- `backend/repositories/warehouse/*`  
- `backend/tests/test_warehouse_layout_*.py`  
- `frontend/app/(dashboard)/inteligencia-bodega/**`  
- `frontend/components/warehouse/**`  
- deps opcionales `konva`, `react-konva` en `frontend/package.json`

### Modificar (mínimo)

- `backend/main.py` — `include_router`  
- `frontend/components/layout/sidebar.tsx` — entrada de menú  
- `frontend/lib/api.ts` — client warehouse  
- `frontend/lib/permissions.ts` — helper capability (si aplica)  
- `backend/sql/069` evolución o migración `070` **solo** si se amplía CHECK de capabilities (alternativa: tabla warehouse propia de ACL)

### No modificar

- PWA Choferes/Vendedores, sync Bsale, picking OC, `products_master` prod, Coolify env sin ticket.

---

## 11. Plan de pruebas (Fase 1)

1. Unit: geometría solape / fuera de sector / generador filas.  
2. API: CRUD draft, publish, concurrencia (409).  
3. Persistencia: logout → login → layout intacto.  
4. Permisos: 403 sin capability/management.  
5. Regresión: góndolas, planificaciones, catálogo, PWA despachos.  
6. Manual desktop: zoom, drag, propiedades, undo/redo.  
7. Migración en PG aislado.

---

## 12. Riesgos y aislamiento

| Riesgo | Mitigación |
|--------|------------|
| Contaminar stock Bsale | Schema propio; cero writes a `bsale_raw.stocks` |
| Confundir piso edificio vs nivel pallet | Campos separados `elevation_m` vs `stack_levels_max` |
| Falsa capacidad | `validation_status`; copy UI: “posible ≠ autorizado” |
| Colisión SQL | Namespace `warehouse/00N` |
| Scope creep Fase 2–7 | Solo rutas layout; sin pantallas fake de pallets/FIFO |
| PWA | No changes Fase 1 |

---

## 13. Implementación incremental (propuesta post-aprobación)

| Slice | Contenido |
|-------|-----------|
| **1.1** | SQL schema + seed BC draft + API read/write elementos |
| **1.2** | Editor Konva: select/move/resize/rotate + properties |
| **1.3** | Generador posiciones + validación geométrica |
| **1.4** | Publish + audit + menú ERP + tests |
| **STOP** | Review gerencial antes de migrar prod / seed real |

---

## 14. Reutilizable vs greenfield

| Reutilizar | Greenfield |
|------------|------------|
| Layout ERP, UI Radix, auth JWT, capabilities pattern, góndolas como *ejemplo* de módulo aislado `app`/`services`, offices Bsale como FK opcional, PM/RAW más adelante | Schema warehouse, editor 2D, generador, versionado layout, nomenclatura BC/IZ/Pn |

---

## 15. Criterio de parada

Auditoría y plan entregados. **No se inicia código** hasta autorización expresa de implementación del slice 1.1+.
