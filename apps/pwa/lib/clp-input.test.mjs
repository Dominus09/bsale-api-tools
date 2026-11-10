import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { formatClpCurrency, formatClpDisplay, parseClpInput } from "./clp-input.ts"

describe("CLP input", () => {
  it("parsea con y sin puntos", () => {
    assert.equal(parseClpInput("50000"), 50000)
    assert.equal(parseClpInput("50.000"), 50000)
    assert.equal(parseClpInput("$90.000"), 90000)
    assert.equal(parseClpInput("15.033"), 15033)
    assert.equal(parseClpInput("155033"), 155033)
  })

  it("formatea visualización", () => {
    assert.equal(formatClpDisplay(50000), "50.000")
    assert.match(formatClpCurrency(155033), /155\.033/)
  })
})
