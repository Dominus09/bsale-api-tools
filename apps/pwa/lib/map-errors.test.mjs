/**
 * Pruebas de clasificación de fallos del mapa.
 * Ejecutar: node --experimental-strip-types --test lib/map-errors.test.mjs
 */
import assert from "node:assert/strict"
import test from "node:test"
import {
  classifyMapFailure,
  classifyMapInitError,
} from "./map-errors.ts"

test("fallo de estilo (crítico) antes de load", () => {
  const c = classifyMapFailure(new Error("Failed to load style from openfreemap"), {
    styleLoaded: false,
  })
  assert.equal(c.kind, "fatal")
  assert.equal(c.code, "map_style_unavailable")
  assert.match(c.messageEs, /lista de clientes/i)
})

test("fallo recuperable de tiles tras load", () => {
  const c = classifyMapFailure(new Error("Failed to fetch tile"), {
    styleLoaded: true,
    sourceId: "openmaptiles",
  })
  assert.equal(c.kind, "recoverable")
  assert.equal(c.code, "map_tiles_degraded")
  assert.match(c.messageEs, /lista de clientes/i)
})

test("tile error con sourceId → recoverable", () => {
  const c = classifyMapFailure("AJAXError: tile 503", {
    styleLoaded: false,
    sourceId: "basemap",
  })
  assert.equal(c.kind, "recoverable")
})

test("mapa no disponible con acceso a lista de clientes", () => {
  const c = classifyMapInitError(new Error("WebGL not supported / maplibre init"))
  assert.equal(c.kind, "fatal")
  assert.match(c.messageEs, /lista de clientes/i)
})

test("error genérico pre-load es fatal", () => {
  const c = classifyMapFailure("boom", { styleLoaded: false })
  assert.equal(c.kind, "fatal")
  assert.equal(c.code, "map_unavailable")
})
