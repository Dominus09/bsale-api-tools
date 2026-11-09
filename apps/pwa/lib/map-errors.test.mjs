/**
 * Pruebas de clasificación de fallos del mapa.
 * Ejecutar: node --experimental-strip-types --test lib/map-errors.test.mjs
 */
import assert from "node:assert/strict"
import test from "node:test"
import {
  classifyMapFailure,
  classifyMapInitError,
  classifyMapTimeout,
} from "./map-errors.ts"

test("GPU / WebGL → fatal", () => {
  const c = classifyMapFailure(new Error("GPUInitializationError: WebGL2 unavailable"), {
    styleLoaded: false,
  })
  assert.equal(c.kind, "fatal")
  assert.equal(c.code, "map_gpu_unavailable")
})

test("fallo de tiles recuperable (no destruir mapa)", () => {
  const c = classifyMapFailure(new Error("Failed to fetch tile"), {
    styleLoaded: false,
    sourceId: "openmaptiles",
  })
  assert.equal(c.kind, "recoverable")
  assert.match(c.messageEs, /lista de clientes/i)
})

test("aviso genérico pre-load ya no es fatal", () => {
  const c = classifyMapFailure("boom", { styleLoaded: false })
  assert.equal(c.kind, "recoverable")
})

test("timeout sin estilo → fatal con acceso a lista", () => {
  const c = classifyMapTimeout()
  assert.equal(c.kind, "fatal")
  assert.match(c.messageEs, /lista de clientes/i)
})

test("init error → fatal", () => {
  const c = classifyMapInitError(new Error("maplibre construct failed"))
  assert.equal(c.kind, "fatal")
})
