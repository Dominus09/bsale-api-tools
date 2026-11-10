import assert from "node:assert/strict"
import { describe, it } from "node:test"

function parseAmount(raw) {
  const t = String(raw ?? "").trim()
  if (!t) return null
  const n = Number(t.replace(",", "."))
  return Number.isFinite(n) ? n : null
}

function sumDeclared(rows) {
  return rows.reduce((acc, r) => {
    if (r.method === "not_applicable") return acc
    const n = parseAmount(r.amount)
    return acc + (n != null && n > 0 ? n : 0)
  }, 0)
}

function pendingBalance(documentTotal, rows) {
  return Math.max(0, Math.round((documentTotal - sumDeclared(rows)) * 100) / 100)
}

function validateMixedPayments(documentTotal, rows) {
  if (!rows.length) return "Agregue al menos una forma de pago"
  let sum = 0
  for (const r of rows) {
    const n = parseAmount(r.amount)
    if (r.method === "not_applicable") {
      if (n != null && n > 0) return "No corresponde no admite monto positivo"
      continue
    }
    if (n == null || n < 0) return "Monto inválido en una forma de pago"
    sum += n
  }
  if (sum - documentTotal > 0.009) return "La suma de montos supera el total del documento"
  return null
}

describe("delivery-collection mixed payments", () => {
  it("acepta cash + pending", () => {
    const rows = [
      { method: "cash", amount: "50000" },
      { method: "pending", amount: "105033" },
    ]
    assert.equal(validateMixedPayments(155033, rows), null)
    assert.equal(sumDeclared(rows), 155033)
    assert.equal(pendingBalance(155033, rows), 0)
  })

  it("acepta cash + transfer + pending", () => {
    const rows = [
      { method: "cash", amount: "50000" },
      { method: "transfer_declared", amount: "70000" },
      { method: "pending", amount: "35033" },
    ]
    assert.equal(validateMixedPayments(155033, rows), null)
    assert.equal(pendingBalance(155033, rows), 0)
  })

  it("rechaza suma sobre el total", () => {
    const rows = [
      { method: "cash", amount: "100000" },
      { method: "pending", amount: "100000" },
    ]
    assert.match(validateMixedPayments(155033, rows) || "", /suma|supera/i)
  })
})
