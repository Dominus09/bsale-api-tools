# Offline V2 — Staging aislado

## Estado de dominio

`pwa-lab.lq-ops.dev` / `api-lab.lq-ops.dev` / `lq-ops.dev` → **NXDOMAIN** (no existen / no controlados).

Usar **túnel HTTPS** (`cloudflared` / ngrok) hacia el stack lab local, o un dominio que el administrador confirme controlar **fuera** de `*.quillotana.cl`.

## Arranque local (esta máquina)

```bash
# 1) Asserts + PG temporal + 061 + API lab + Next
python deploy/staging-offline-v2/start_local_lab_stack.py

# 2) En otra consola: túnel HTTPS a PWA (ejemplo cloudflared)
cloudflared tunnel --url http://127.0.0.1:13015
```

Writes arrancan **OFF**. Para piloto staging:

```bash
python deploy/staging-offline-v2/toggle_staging_writes.py --on --allowlist 42 --i-confirm-staging
# reiniciar API con esas env
```

Kill switch:

```bash
python deploy/staging-offline-v2/toggle_staging_writes.py --off
```

## Coolify

**No** desplegar sobre servicios `api/app/work/test.quillotana.cl`.

Crear proyecto Coolify **nuevo** solo si el admin confirma:

1. Sin webhook a `main` productivo, o rama `lab/offline-v2-staging` aislada.
2. PG dedicado (no analytics).
3. Bucket R2 `*-lab-*`.
4. JWT distinto.
5. Sin `BSALE_TOKEN` productivo.

## Prohibido

Push `origin/main`, SQL prod, R2 prod, Bsale prod, visitas reales.
