import assert from "node:assert/strict"
import { describe, it } from "node:test"

function resolveReceptorName(domValue, stateValue, formDataValue) {
  const dom = String(domValue ?? "").trim()
  if (dom) return dom
  const fromForm = String(formDataValue ?? "").trim()
  if (fromForm) return fromForm
  return String(stateValue ?? "").trim()
}

function receptorNameIsValid(name, refused) {
  if (refused) return true
  return name.trim().length >= 2
}

describe("resolveReceptorName (hotfix iOS autofill)", () => {
  it("usa DOM cuando state está vacío (autofill sin onChange)", () => {
    assert.equal(resolveReceptorName("Juan Pérez", ""), "Juan Pérez")
  })

  it("usa FormData si DOM vacío y state vacío", () => {
    assert.equal(resolveReceptorName("", "", "Ana López"), "Ana López")
  })

  it("prioriza DOM sobre FormData y state", () => {
    assert.equal(resolveReceptorName(" DOM ", "state", "form"), "DOM")
  })

  it("prioriza FormData sobre state", () => {
    assert.equal(resolveReceptorName("", "state", " form "), "form")
  })

  it("valida longitud mínima salvo negativa", () => {
    assert.equal(receptorNameIsValid("A", false), false)
    assert.equal(receptorNameIsValid("Ana", false), true)
    assert.equal(receptorNameIsValid("", true), true)
  })

  it("simula iPhone: visible en DOM, state vacío → submit OK", () => {
    const state = ""
    const domVisible = "Receptor iPhone"
    const resolved = resolveReceptorName(domVisible, state, null)
    assert.equal(receptorNameIsValid(resolved, false), true)
    assert.equal(resolved, "Receptor iPhone")
  })
})
