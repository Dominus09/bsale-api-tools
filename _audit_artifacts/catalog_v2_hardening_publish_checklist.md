# Catálogo V2 — Hardening + checklist post-push

## Cambios de hardening
1. **UPB:** no inventar 18 para Sol; capas sellable / presentación / logística; `pending_validation` sin PM/columna.
2. **Fallo V2:** HTTP 503 `catalog_v2_unavailable` — **sin** fallback silencioso a legacy.
3. **Flag público:** default `legacy` / unset.
4. Procedimiento Bsale sin tokens en repo.

## Checklist activación controlada (futuro, otra autorización)
- [ ] `python -m backend.scripts.catalog_v2_bsale_point_check` en entorno con token
- [ ] Contrastar Mariposa / Sidra 750 / Sol x18 (avail office 1 + lista 13)
- [ ] Revisar `/api/catalog-v2/compare` gerencia
- [ ] Validar Sol UPB logístico en PM **manual** (sin auto-write)
- [ ] Set Coolify `CATALOG_SOURCE_PUBLIC=v2` (solo entonces)
- [ ] Monitorear 503 vs tráfico; rollback = unset / `legacy`
- [ ] **No** tocar picking / OC / Products Master prod en este paso

## Rollback inmediato
`CATALOG_SOURCE_PUBLIC=legacy` o eliminar variable. Redeploy solo si Coolify exige reload de env (autorización aparte).
