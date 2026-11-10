import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  isDeclarativeCollection,
  paymentMethodLabel,
  validatePaymentMethod,
  validateMixedPayments,
  sumDeclared,
  pendingBalance,
} from "./delivery-collection.ts"

describe("delivery-collection declarative", () => {
  it("valida métodos operativos sin montos", () => {
    assert.equal(validatePaymentMethod("cash"), null)
    assert.equal(validatePaymentMethod("cash_and_transfer"), null)
    assert.equal(validatePaymentMethod("pending"), null)
    assert.match(validatePaymentMethod("transfer_verified") || "", /Seleccione/)
    assert.match(validatePaymentMethod("") || "", /Seleccione/)
  })

  it("etiquetas legibles", () => {
    assert.equal(paymentMethodLabel("cash"), "Efectivo")
    assert.equal(paymentMethodLabel("cash_and_transfer"), "Efectivo + transferencia")
    assert.equal(paymentMethodLabel("not_applicable"), "Sin cobro / No aplica")
  })

  it("detecta filas declarativas (sin monto)", () => {
    assert.equal(isDeclarativeCollection({ method: "cash", declared_amount: null }), true)
    assert.equal(isDeclarativeCollection({ method: "cash", declared_amount: 1000 }), false)
  })
})

describe("delivery-collection mixed payments (legado)", () => {
  it("acepta cash + pending", () => {
    const rows = [
      { key: "1", method: "cash", amount: "50000", note: "" },
      { key: "2", method: "pending", amount: "105033", note: "" },
    ]
    assert.equal(validateMixedPayments(155033, rows), null)
    assert.equal(sumDeclared(rows), 155033)
    assert.equal(pendingBalance(155033, rows), 0)
  })

  it("rechaza suma sobre el total", () => {
    const rows = [
      { key: "1", method: "cash", amount: "100000", note: "" },
      { key: "2", method: "pending", amount: "100000", note: "" },
    ]
    assert.match(validateMixedPayments(155033, rows) || "", /suma|supera/i)
  })
})
