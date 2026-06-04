# Coolify — primer job oficial: `sync_distribuidora_related`

Job que sincroniza relaciones operacionales OC → documentos (vía `relateddetailid`) hacia PostgreSQL (`distribuidora.document_related`). No programa cron ni schedules en el código; en Coolify se define **cuándo** ejecutarlo (manual o según su política de despliegue).

## Comando exacto

Desde el **directorio raíz del repositorio** (donde Python resuelve el paquete `backend`):

```bash
python -m backend.jobs.sync_distribuidora_related
```

Requisito: mismo entorno que el backend (dependencias instaladas, `PYTHONPATH` implícito al ejecutar desde la raíz del proyecto o imagen que ya incluya el código).

## Variables de entorno necesarias

| Variable | Obligatoria | Descripción |
|----------|-------------|-------------|
| `PG_HOST` | Sí | Host PostgreSQL |
| `PG_DB` | Sí | Base de datos |
| `PG_USER` | Sí | Usuario |
| `PG_PASSWORD` | Sí | Contraseña |
| `PG_PORT` | No | Por defecto `5432` |
| `BSALE_TOKEN` o `BSALE_TOKEN_SPA` | Sí | Token API Bsale (el job usa `strict_token=True`) |

Opcionales (comportamiento del sync):

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DISTRIBUIDORA_RELATED_LOOKBACK_DAYS` | `7` | Días hacia atrás para elegir OC por `emission_date` |
| `DISTRIBUIDORA_RELATED_DETAIL_LIMIT` | `250` | Máximo de **documentos** OC a considerar por corrida (nombre histórico de la variable) |
| `DISTRIBUIDORA_RELATED_API_DELAY_SEC` | `0.12` | Pausa entre llamadas API para no saturar Bsale |
| `LOG_LEVEL` | `INFO` | Nivel de logging (`DEBUG`, `INFO`, …) |
| `DISTRIBUIDORA_RELATED_EXIT_CODE_ON_LOCK` | `0` | Si otra instancia tiene el advisory lock de related: `0` = éxito “sin trabajo”; `1` = fallo controlado para alertas |
| `DISTRIBUIDORA_RELATED_EXIT1_ON_DOC_ERRORS` | (vacío) | Si `1` / `true` / `yes`: salida `1` cuando hubo errores por documento (parciales) |

Cargar `.env`: el módulo intenta `load_dotenv()` si `python-dotenv` está instalado; en Coolify suele bastar con definir las variables en el servicio/job.

## Frecuencia recomendada

Orientativo: **cada 15–60 minutos** en horario operativo, o **1–4 veces al día** si el volumen de OC es bajo y el lookback cubre bien la ventana. Ajustar según cuántas OC nuevas/cerradas hay y el valor de `DISTRIBUIDORA_RELATED_LOOKBACK_DAYS`.

## Timeout recomendado

- Mínimo razonable: **15 minutos**.
- Con `DISTRIBUIDORA_RELATED_DETAIL_LIMIT=250`, lookback 7 días y delay API por defecto, suele bastar **30–45 minutos** como techo cómodo.
- Si suben límite o lookback, aumentar timeout proporcionalmente.

## Recursos recomendados

- **CPU / RAM**: perfil ligero (1 vCPU, **256–512 MiB** RAM suele ser suficiente). El cuello de botella es red hacia Bsale y PostgreSQL, no cómputo intenso.
- **Red**: acceso saliente a la API Bsale y al `PG_HOST`.

## Códigos de salida

| Código | Significado |
|--------|-------------|
| `0` | Éxito (incluye corrida omitida por lock si `DISTRIBUIDORA_RELATED_EXIT_CODE_ON_LOCK=0`) |
| `1` | Error controlado: token faltante, excepción no recuperada, `skipped`, lock con exit 1, o errores por documento si `DISTRIBUIDORA_RELATED_EXIT1_ON_DOC_ERRORS` está activado |

## Concurrencia

El servicio usa un **advisory lock** dedicado (`pg_try_advisory_lock`) para no solapar otra corrida del mismo tipo de sync sobre `document_related`. No ejecutar dos instancias del mismo job en paralelo salvo que acepten la omisión controlada vía variables anteriores.

## Observabilidad

El job escribe logs en **stdout** y un bloque final **RESUMEN JOB** con documentos considerados, detalles, items related, filas insertadas, llamadas API, duración y contadores de errores por documento.

---

## Job catálogo Bsale + `products_master`: `sync_bsale_catalog`

Mantiene `bsale.variants` / precios / stock al día y hace UPSERT incremental en `bsale.products_master` **sin borrar filas ni pisar cubicación manual**.

### Comando

```bash
python -m backend.jobs.sync_bsale_catalog
```

### Variables

| Variable | Obligatoria | Descripción |
|----------|-------------|-------------|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD` | Sí | PostgreSQL |
| Tokens Bsale usados por `sync_catalog.py` / precios / stock | Sí | Misma convención que los scripts en la raíz del repo |

### DDL previo

Ejecutar una vez: `backend/sql/032_products_master_logistics.sql` (ver `docs/PRODUCTS_MASTER_SYNC.md`).

### Frecuencia y timeout

- **1–2 ejecuciones/día** suele bastar para catálogo estable.
- Timeout recomendado: **45–90 min**.

### Logs

Prefijo `[CATALOG_SYNC]` en stdout: insertados/actualizados en `products_master`, `units_per_box` desde SEC, errores.
