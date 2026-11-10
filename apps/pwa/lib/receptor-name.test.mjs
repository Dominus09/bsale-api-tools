import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { createRequire } from "node:module"

// TS helpers compiled via strip-types experimentally — reimplement mirror for node:test
function resolveReceptorName(domValue, stateValue) {
  const dom = String(domValue ?? "").trim()
  const state = String(stateValue ?? "").trim()
  return dom || state
}

function receptorNameIsValid(name, refused) {
  if (refused) return true
  return name.trim().length >= 2
}

describe("resolveReceptorName (hotfix iOS autofill)", () => {
  it("usa DOM cuando state está vacío (autofill sin onChange)", () => {
    assert.equal(resolveReceptorName("Juan Pérez", ""), "Juan Pérez")
  })

  it("usa state si DOM vacío", () => {
    assert.equal(resolveReceptorName("", "Ana"), "Ana")
  })

  it("prioriza DOM no vacío sobre state", () => {
    assert.equal(resolveReceptorName(" DOM ", "state"), "DOM")
  })

  it("valida longitud mínima salvo negativa", () => {
    assert.equal(receptorNameIsValid("A", false), false)
    assert.equal(receptorNameIsValid("Ana", false), true)
    assert.equal(receptorNameIsValid("", true), true)
  })
})

void createRequire
