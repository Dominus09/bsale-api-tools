import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { resolveSemaphore } from "./semaphore.ts"

describe("semaphore", () => {
  it("verde completa", () => {
    const s = resolveSemaphore({ status: "completed" })
    assert.equal(s.color, "green")
  })
  it("amarillo parcial", () => {
    const s = resolveSemaphore({ status: "partial" })
    assert.equal(s.color, "yellow")
  })
  it("rojo local cerrado específico", () => {
    const s = resolveSemaphore({ status: "not_delivered", resultReason: "closed_store" })
    assert.equal(s.color, "red")
    assert.equal(s.labelEs, "Local cerrado")
  })
  it("rojo pedido rechazado", () => {
    const s = resolveSemaphore({ status: "not_delivered", resultReason: "rejected_order" })
    assert.equal(s.labelEs, "Pedido rechazado")
  })
  it("neutro pendiente", () => {
    const s = resolveSemaphore({ status: null })
    assert.equal(s.color, "neutral")
  })
})
