import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")
const offlineHtml = readFileSync(join(root, "public", "offline.html"), "utf8")

describe("service worker offline shell", () => {
  it("usa versión v9+ Safari no-redirect y precachea shell + offline.html", () => {
    assert.match(sw, /lq-pwa-v9-safari-no-redirect/)
    assert.match(sw, /OFFLINE_HTML\s*=\s*"\/offline\.html"/)
    assert.match(sw, /APP_SHELL\s*=\s*"\/despachos"/)
    assert.match(sw, /asCleanResponse/)
    assert.match(sw, /Response served by service worker has redirections|redirected/)
    assert.match(sw, /X-LQ-Offline-RSC/)
    // No precachear "/" (redirect Next → /despachos).
    const precacheStart = sw.indexOf("const PRECACHE = [")
    const precacheEnd = sw.indexOf("];", precacheStart)
    const precacheBody = sw.slice(precacheStart, precacheEnd)
    assert.doesNotMatch(precacheBody, /"\/"/)
  })

  it("nunca deja respondWith con null (navegación y assets)", () => {
    assert.match(sw, /syntheticOfflineHtml/)
    assert.match(sw, /safeOfflineNavigation/)
    assert.doesNotMatch(sw, /respondWith\(\s*null\s*\)/)
  })

  it("NO sirve APP_SHELL HTML para rutas distintas (evita client-side exception)", () => {
    assert.match(sw, /NUNCA devolver APP_SHELL/)
    assert.match(sw, /safeOfflineNavigation\(request\)/)
    const fnStart = sw.indexOf("async function safeOfflineNavigation")
    const fnEnd = sw.indexOf("async function putClean", fnStart)
    const body = sw.slice(fnStart, fnEnd)
    assert.match(body, /path === APP_SHELL/)
    assert.match(body, /OFFLINE_HTML/)
    assert.doesNotMatch(
      body,
      /matchFirst\(\[\s*request\.url,\s*APP_SHELL/,
    )
  })

  it("no cachea redirects ni login como HTML de fichas", () => {
    assert.match(sw, /isCacheableAppHtml/)
    assert.match(sw, /isLoginPath/)
    assert.match(sw, /response\.redirected/)
    assert.match(sw, /putClean/)
  })

  it("chunks JS offline: 503 explícito sin módulo ficticio / sin HTML", () => {
    assert.match(sw, /missingOfflineChunkResponse/)
    assert.match(sw, /X-LQ-Offline-Chunk-Missing/)
    assert.doesNotMatch(sw, /syntheticOfflineScript/)
    const fnStart = sw.indexOf("function missingOfflineChunkResponse")
    const fnEnd = sw.indexOf("async function matchFirst", fnStart)
    const body = sw.slice(fnStart, fnEnd)
    assert.doesNotMatch(body, /application\/javascript/)
  })

  it("no intercepta API ni login", () => {
    assert.match(sw, /api-upstream/)
    assert.match(sw, /isApiPath/)
    assert.match(sw, /pathname === "\/login"/)
    assert.match(sw, /if \(isApiPath\(url\.pathname\)\) return;/)
  })

  it("offline.html distingue V1 consulta / V2 captura / sync", () => {
    assert.match(offlineHtml, /Sin conexión/)
    assert.match(offlineHtml, /href="\/despachos"/)
    assert.match(offlineHtml, /Offline V1/)
    assert.match(offlineHtml, /Offline V2/)
    assert.match(offlineHtml, /sincronizaci/i)
    assert.doesNotMatch(
      offlineHtml,
      /Las entregas, GPS, fotografías y WhatsApp requieren conexión/,
    )
  })
})
