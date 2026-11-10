import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { breakdownBoxUnits } from "./box-units.ts"

describe("box-units", () => {
  it("60 un con factor 12 → 5 cajas exactas", () => {
    const b = breakdownBoxUnits(60, 12, { equivalenceStatus: "verified" })
    assert.equal(b.available, true)
    assert.equal(b.fullBoxes, 5)
    assert.equal(b.looseUnits, 0)
    assert.equal(b.label, "5 cajas × 12 unidades")
  })

  it("62 un → 5 cajas + 2 unidades", () => {
    const b = breakdownBoxUnits(62, 12)
    assert.equal(b.label, "5 cajas + 2 unidades")
  })

  it("sin factor no inventa cajas", () => {
    const b = breakdownBoxUnits(60, null, { sinUnidadCaja: true })
    assert.equal(b.available, false)
    assert.equal(b.label, null)
  })
})
