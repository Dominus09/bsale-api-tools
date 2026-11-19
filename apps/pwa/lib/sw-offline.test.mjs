import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")
const offlineHtml = readFileSync(join(root, "public", "offline.html"), "utf8")

describe("service worker offline shell", () => {
  it("usa versión v8+ y precachea shell + offline.html", () => {
    assert.match(sw, /lq-pwa-v8-offline-route-guard/)
    assert.match(sw, /OFFLINE_HTML\s*=\s*"\/offline\.html"/)
    assert.match(sw, /APP_SHELL\s*=\s*"\/despachos"/)
    assert.match(sw, /const PRECACHE = \[\s*OFFLINE_HTML/)
    assert.match(sw, /X-LQ-Offline-RSC/)
  })

  it("nunca deja respondWith con null (navegación y assets)", () => {
    assert.match(sw, /syntheticOfflineHtml/)
    assert.match(sw, /safeOfflineNavigation/)
    assert.doesNotMatch(sw, /respondWith\(\s*null\s*\)/)
  })

  it("NO sirve APP_SHELL HTML para rutas distintas (evita client-side exception)", () => {
    assert.match(sw, /NUNCA devolver APP_SHELL/)
    assert.match(sw, /safeOfflineNavigation\(request\)/)
    // El fallback genérico offline no debe listar APP_SHELL junto a rutas profundas.
    const fnStart = sw.indexOf("async function safeOfflineNavigation")
    const fnEnd = sw.indexOf("async function putCopy", fnStart)
    const body = sw.slice(fnStart, fnEnd)
    assert.match(body, /path === APP_SHELL/)
    assert.match(body, /OFFLINE_HTML/)
    // Para path !== shell, solo offline.html — no devolver /despachos por defecto.
    assert.doesNotMatch(
      body,
      /matchFirst\(\[\s*request\.url,\s*APP_SHELL/,
    )
  })

  it("chunks JS offline no devuelven text/plain (SyntaxError)", () => {
    assert.match(sw, /syntheticOfflineScript/)
    assert.match(sw, /X-LQ-Offline-Chunk/)
    assert.match(sw, /application\/javascript/)
  })

  it("no intercepta API ni login", () => {
    assert.match(sw, /api-upstream/)
    assert.match(sw, /isApiPath/)
    assert.match(sw, /pathname === "\/login"/)
    assert.match(sw, /if \(isApiPath\(url\.pathname\)\) return;/)
  })

  it("offline.html es shell estático usable", () => {
    assert.match(offlineHtml, /Sin conexión/)
    assert.match(offlineHtml, /href="\/despachos"/)
    assert.doesNotMatch(offlineHtml, /P1 no se guardan/)
  })
})
