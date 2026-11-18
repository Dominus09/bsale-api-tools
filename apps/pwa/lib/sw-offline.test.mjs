import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")
const offlineHtml = readFileSync(join(root, "public", "offline.html"), "utf8")

describe("service worker offline shell", () => {
  it("usa versión v4+ y precachea offline.html", () => {
    assert.match(sw, /lq-pwa-v4-offline-shell/)
    assert.match(sw, /OFFLINE_HTML\s*=\s*"\/offline\.html"/)
    assert.match(sw, /const PRECACHE = \[\s*OFFLINE_HTML/)
  })

  it("nunca deja respondWith con null (navegación y assets)", () => {
    assert.match(sw, /syntheticOfflineHtml/)
    assert.match(sw, /safeOfflineNavigation/)
    assert.doesNotMatch(sw, /respondWith\(\s*null\s*\)/)
    assert.match(sw, /return hit \|\| syntheticOfflineHtml\(\)/)
    assert.match(sw, /return pageHit \|\| syntheticOfflineHtml\(\)/)
    assert.match(sw, /return again \|\| syntheticOfflineText\(\)/)
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
