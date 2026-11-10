#!/usr/bin/env node
/**
 * Prueba repetida de conectividad upstream (detecta EAI_AGAIN intermitente).
 *
 * Uso (desde contenedor PWA o máquina con red Docker):
 *   node scripts/probe-api-upstream.mjs
 *   API_PROXY_TARGET=http://<nombre-interno>:8000 node scripts/probe-api-upstream.mjs
 *   PROBE_URL=https://api.quillotana.cl PROBE_N=30 PROBE_MS=500 node scripts/probe-api-upstream.mjs
 *
 * No escribe datos. Solo GET a rutas que suelen responder 401/405/422.
 */
const base = (
  process.env.PROBE_URL ||
  process.env.API_PROXY_TARGET ||
  process.env.NEXT_PUBLIC_API_URL ||
  "https://api.quillotana.cl"
).replace(/\/$/, "")

const n = Math.max(1, Number(process.env.PROBE_N || 20))
const delayMs = Math.max(0, Number(process.env.PROBE_MS || 400))
const paths = (process.env.PROBE_PATHS || "/login,/docs,/openapi.json").split(",")

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function once(path) {
  const url = `${base}${path.startsWith("/") ? path : `/${path}`}`
  const t0 = Date.now()
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "manual",
      cache: "no-store",
      signal: AbortSignal.timeout(8000),
    })
    return {
      ok: true,
      status: res.status,
      ms: Date.now() - t0,
      error: null,
    }
  } catch (err) {
    const code =
      err && typeof err === "object" && "cause" in err && err.cause && typeof err.cause === "object"
        ? String(err.cause.code || "")
        : err && typeof err === "object" && "code" in err
          ? String(err.code || "")
          : ""
    const msg = err instanceof Error ? err.message : String(err)
    return {
      ok: false,
      status: 0,
      ms: Date.now() - t0,
      error: code || msg.slice(0, 120),
    }
  }
}

const stats = { ok: 0, fail: 0, byError: {}, byStatus: {} }

console.log(JSON.stringify({ probe_base: base, rounds: n, paths }, null, 0))

for (let i = 1; i <= n; i++) {
  const path = paths[(i - 1) % paths.length].trim() || "/login"
  const r = await once(path)
  if (r.ok) {
    stats.ok += 1
    stats.byStatus[r.status] = (stats.byStatus[r.status] || 0) + 1
  } else {
    stats.fail += 1
    stats.byError[r.error] = (stats.byError[r.error] || 0) + 1
  }
  console.log(
    `#${i} ${path} → ${r.ok ? `HTTP ${r.status}` : `FAIL ${r.error}`} (${r.ms}ms)`,
  )
  if (delayMs) await sleep(delayMs)
}

const summary = {
  base,
  rounds: n,
  ok: stats.ok,
  fail: stats.fail,
  fail_rate: Number((stats.fail / n).toFixed(3)),
  by_status: stats.byStatus,
  by_error: stats.byError,
  verdict:
    stats.fail === 0
      ? "stable"
      : stats.fail / n >= 0.1
        ? "intermittent_or_down"
        : "mostly_ok_with_blips",
}
console.log("---")
console.log(JSON.stringify(summary, null, 2))
process.exit(stats.fail > 0 ? 2 : 0)
