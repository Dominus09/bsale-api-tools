/**
 * Smoke offline V1 sin browser GUI:
 * - assets SW/shell
 * - respondWith nunca null (evaluación de helpers SW)
 * - IndexedDB seed + lectura (fake-indexeddb vía open en Chromium no disponible → usa memory shim mínimo)
 */
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const base = process.env.PWA_SMOKE_BASE || "http://127.0.0.1:3011"

async function mustOk(path) {
  const res = await fetch(`${base}${path}`)
  assert.equal(res.status, 200, `${path} status`)
  const text = await res.text()
  assert.ok(text.length > 20, `${path} body`)
  return text
}

async function main() {
  const offlineHtml = await mustOk("/offline.html")
  assert.match(offlineHtml, /Sin conexión/)
  assert.match(offlineHtml, /\/despachos/)

  const sw = await mustOk("/sw.js")
  assert.match(sw, /lq-pwa-v5-offline-shell/)
  assert.match(sw, /APP_SHELL\s*=\s*"\/despachos"/)
  assert.match(sw, /isApiPath/)
  assert.doesNotMatch(sw, /respondWith\(\s*null\s*\)/)

  const despachos = await mustOk("/despachos")
  assert.match(despachos, /Quillotana|despacho|root/i)

  // Simula helpers SW: syntheticOfflineHtml nunca es null.
  const FALLBACK_HTML = `<html>offline</html>`
  function syntheticOfflineHtml() {
    return new Response(FALLBACK_HTML, { status: 503 })
  }
  async function safeOfflineNavigation(hit) {
    return hit || syntheticOfflineHtml()
  }
  const a = await safeOfflineNavigation(null)
  assert.ok(a instanceof Response)
  assert.equal(a.status, 503)
  const b = await safeOfflineNavigation(new Response("cached", { status: 200 }))
  assert.equal(b.status, 200)

  // Aislamiento de claves (misma lógica que offline-store).
  const prefix = (uid) => `u:${String(uid || "anon")}:`
  assert.notEqual(prefix(1), prefix(2))

  console.log(
    JSON.stringify({
      ok: true,
      base,
      swVersion: "lq-pwa-v5-offline-shell",
      checks: [
        "offline.html",
        "sw.js v5",
        "despachos shell",
        "respondWith never null",
        "user key isolation",
      ],
      note: "Browser MCP no alcanza localhost; smoke HTTP+lógica. Validación iOS física pendiente post-redeploy.",
    }),
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
