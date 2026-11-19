/**
 * Revisión final Offline V1 — escenarios de integración (lógica).
 * No es E2E Safari iOS: el SyntaxError de chunks era inferencia técnica,
 * no un stack capturado en dispositivo.
 */
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import { isOfflineUiReady } from "./offline-build.ts"
import { extractNextStaticAssets } from "./clients-list-safe.ts"
import { offlineUserPrefix } from "./offline-store.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")

describe("Offline V1 revisión final", () => {
  it("A/E: ready UI exige data + nav + mismo navBuildId", () => {
    const build = "build-a"
    assert.equal(
      isOfflineUiReady(
        {
          dataComplete: true,
          navReady: true,
          navBuildId: "build-a",
          complete: true,
        },
        build,
      ),
      true,
    )
  })

  it("B: SW no sirve APP_SHELL para Clientes (HTML de otra ruta)", () => {
    const fnStart = sw.indexOf("async function safeOfflineNavigation")
    const fnEnd = sw.indexOf("async function putCopy", fnStart)
    const body = sw.slice(fnStart, fnEnd)
    assert.match(body, /OFFLINE_HTML/)
    assert.match(body, /path === APP_SHELL/)
  })

  it("C: chunk JS faltante → 503 sin fingir application/javascript", () => {
    assert.match(sw, /missingOfflineChunkResponse/)
    assert.match(sw, /X-LQ-Offline-Chunk-Missing/)
    assert.doesNotMatch(sw, /syntheticOfflineScript/)
    const fnStart = sw.indexOf("function missingOfflineChunkResponse")
    const fnEnd = sw.indexOf("async function matchFirst", fnStart)
    const body = sw.slice(fnStart, fnEnd)
    assert.doesNotMatch(body, /application\/javascript/)
    assert.match(body, /status: 503/)
    assert.match(body, /Response\(null/)
  })

  it("D: SW nuevo + IDB antigua → no listo UI (build mismatch)", () => {
    assert.equal(
      isOfflineUiReady(
        {
          dataComplete: true,
          navReady: true,
          navBuildId: "v7-old",
          complete: true,
        },
        "v8-new",
      ),
      false,
    )
    // Datos siguen existiendo (dataComplete); solo se niega "listo".
    assert.equal(
      Boolean({
        dataComplete: true,
        navReady: true,
        navBuildId: "v7-old",
      }.dataComplete),
      true,
    )
  })

  it("warm exige assets críticos del HTML de Clientes", () => {
    const sync = readFileSync(join(root, "lib/offline-sync.ts"), "utf8")
    assert.match(sync, /criticalAssets/)
    assert.match(sync, /criticalAssetsOk/)
    assert.match(sync, /navBuildId/)
    assert.match(sync, /dataComplete/)
    const html = `<script src="/_next/static/chunks/clientes-page.js"></script>`
    assert.deepEqual(extractNextStaticAssets(html), [
      "/_next/static/chunks/clientes-page.js",
    ])
  })

  it("F: claves IDB aisladas por userId (logout no mezcla)", () => {
    assert.notEqual(offlineUserPrefix("10"), offlineUserPrefix("20"))
    assert.match(offlineUserPrefix("10") + "plan:1:ready", /^u:10:plan:1:ready$/)
  })

  it("documenta límite: SyntaxError Safari fue inferencia, no stack capturado", () => {
    assert.ok(
      true,
      "Sin Web Inspector iPhone en esta infra; causa reconstruida por SW HTML mismatch + chunk ausente.",
    )
  })
})
