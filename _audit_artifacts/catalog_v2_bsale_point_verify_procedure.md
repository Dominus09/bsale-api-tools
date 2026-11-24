# Procedimiento seguro: verificación puntual Bsale (pre-activación V2)

**No** guardar tokens en el repo. **No** pegar credenciales en chats/logs. **No** imprimir `access_token`.

## Objetivo
Contrastar SKU críticos (stock office 1 + precio lista 13) contra Bsale API **antes** de `CATALOG_SOURCE_PUBLIC=v2`.

## Entorno autorizado
1. Shell en servidor/API con secretos Coolify (o workstation con env inyectado).
2. Exportar solo en la sesión:
   ```bash
   # valor desde secret manager / Coolify UI — no commitear
   export BSALE_TOKEN='…'
   ```
3. Verificar que el proceso no echoa el token:
   ```bash
   python -c "import os; t=os.getenv('BSALE_TOKEN',''); print('token_len', len(t))"
   ```

## Consulta puntual (script del repo)
```bash
python -m backend.scripts.catalog_v2_bsale_point_check \
  28216 29094 31199 28258 30814 31339
```
Compara mentalmente con:
- `bsale_raw.stocks` office 1 → `quantityAvailable`
- `GET /api/catalog-v2/compare?variant_ids=…` (gerencia)

## Criterio de aceptación por SKU
| Campo | Regla |
|---|---|
| company | token empresa 3 |
| office | 1 |
| variant_id | exacto |
| stock público | `quantityAvailable` (no `quantity`) |
| reservas | anotar `quantityReserved` |
| precio | lista 13 gross vs RAW/legacy |

Si API ≠ RAW payload reciente → **no activar** público; investigar sync stocks/prices.

## Limpieza
```bash
unset BSALE_TOKEN
history -c  # o equivalente en PowerShell: quitar de historial
```

## Activación (otro ticket)
Solo tras contraste OK: setear `CATALOG_SOURCE_PUBLIC=v2` en Coolify **manualmente**.  
Rollback: `CATALOG_SOURCE_PUBLIC=legacy` o unset.
