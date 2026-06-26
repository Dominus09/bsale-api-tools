# Coolify — deploy frontend (work.quillotana.cl)

## Configuración del recurso

| Campo | Valor |
|-------|--------|
| Build Pack | **Dockerfile** (no Nixpacks) |
| Base Directory | `frontend` |
| Dockerfile | `Dockerfile` |
| Puerto | `3000` |

No usar el `Dockerfile` de la raíz del repo (es el API Python en puerto 8000).

## Error: `exit code 255` en `npm ci`

Suele ser **falta de RAM** en el servidor de build o timeout de red al registry de npm.

### 1. Reducir memoria del build (Coolify → Build Arguments)

```
BUILD_NODE_MEMORY_MB=2048
```

En VPS con 2 GB RAM, probar `1536`.

### 2. Swap en el servidor (recomendado)

```bash
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### 3. Ver log completo

En Coolify: **Show Debug Logs** en el deploy fallido. El mensaje real (OOM, ECONNRESET, lockfile) aparece después de `npm ci`.

### 4. Build local y push de imagen (alternativa)

Si el VPS no puede compilar Next.js:

```bash
cd frontend
docker build -t bsale-frontend:latest .
docker save bsale-frontend:latest | ssh servidor docker load
```

## Variables de entorno runtime

| Variable | Descripción |
|----------|-------------|
| `NEXT_PUBLIC_API_URL` | URL del API FastAPI (ej. `https://api.quillotana.cl`) |
| `PORT` | `3000` (Coolify suele inyectarlo) |

## Healthcheck

El contenedor expone `GET /` en puerto 3000. El Dockerfile incluye healthcheck con `curl`.
