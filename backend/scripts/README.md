# Scripts (compatibilidad FASE 6)

Los módulos reales viven en:

- `backend/audits/` — auditorías
- `backend/maintenance/` — limpieza y hashes
- `backend/debug/` — diagnóstico y exports

Los `.py` en **esta** carpeta son **shims** que reenvían al módulo canónico para no romper rutas o documentación antigua.

Preferir siempre:

```bash
python -m backend.audits.audit_document_related
python -m backend.debug.debug_document_types
```

Ver `backend/STRUCTURE_MIGRATION_REPORT.md`.
