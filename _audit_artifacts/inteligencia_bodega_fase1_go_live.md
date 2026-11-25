# Inteligencia de Bodega — Fase 1 Go-Live

**Actualizado:** 2026-11-25 13:07 (UTC-3)  
**Estado:** verificaciones finales OK · commit/push autorizados · redeploy Coolify **manual por el usuario**

---

## 0. Infraestructura

| Rol | Destino |
|-----|---------|
| ERP | `https://test.quillotana.cl` |
| API | `https://api.quillotana.cl` |
| PostgreSQL | `analytics` |
| `work.quillotana.cl` | En desuso — ignorar |

---

## 1. Verificaciones confirmadas (usuario + MCP catálogo)

| Ítem | Resultado |
|------|-----------|
| Backup PostgreSQL | Confirmado |
| SQL 001–004 en `analytics` | Aplicados (agente no re-ejecutó) |
| VERIFY 005 | Ejecutado |
| Schema + 6 tablas + índices draft/published + columnas publish | OK (MCP `pg_catalog`) |
| `draft_bc` | **1** |
| `published_bc` | **0** |
| `layout_elements_in_draft` | **17** |
| 17 elementos físicos BC | Confirmado |
| `PG_USER` FastAPI | **`postgres`** (ACL warehouse alcanza) |
| Suite warehouse aislada | 33 passed |
| ESLint warehouse | 0 |
| Next build | OK (`/inteligencia-bodega`) |

---

## 2. Deploy

- Push a `main` autorizado.
- **Sin** redeploy automático desde el agente.
- Redeploy **manual** en Coolify: API → luego ERP (`test.quillotana.cl`).
- Coolify UI no verificable desde este entorno (`gh` ausente); orden de despliegue queda bajo control manual del usuario.
- Seed permanece **draft** (no publicar automáticamente).
- Sin Fase 2 · sin PWA · sin módulos ajenos.

---

## 3. Commit

Mensaje: `feat(warehouse): Editor Maestro Fase 1`  
Incluye solo paths Inteligencia de Bodega (backend warehouse, FE editor, SQL, tests, audit artifacts, sidebar/main/package konva).
