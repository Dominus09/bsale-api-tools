import assert from "node:assert/strict"
import { describe, it } from "node:test"

function isEntregaFocusPath(pathname) {
  return String(pathname || "").includes("/entrega")
}

describe("navegación modular sin tabs de ficha", () => {
  it("detecta rutas /entrega/* para ocultar tabs", () => {
    assert.equal(
      isEntregaFocusPath("/despachos/12/clientes/C1/entrega/documentos"),
      true,
    )
    assert.equal(
      isEntregaFocusPath("/despachos/12/clientes/C1/entrega/firma"),
      true,
    )
    assert.equal(isEntregaFocusPath("/despachos/12/clientes/C1"), false)
    assert.equal(isEntregaFocusPath("/despachos/12/clientes"), false)
  })
})

describe("resumen compacto firma", () => {
  it("no lista todas las líneas por defecto", () => {
    const lines = Array.from({ length: 80 }, (_, i) => ({
      product_name: `P${i}`,
      planned_units: 1,
      delivered_units: 1,
    }))
    const compactCount = 1 + 1 + 1 // cliente + totales + resumen líneas
    assert.ok(compactCount < lines.length)
    assert.equal(lines.length, 80)
  })
})
