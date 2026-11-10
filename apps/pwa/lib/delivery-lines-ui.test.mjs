import assert from "node:assert/strict"
import { describe, it } from "node:test"

function lineOutcome(row) {
  if (!row.persisted) return null
  const planned = Number(row.planned) || 0
  const delivered = Number(row.delivered) || 0
  if (delivered >= planned) return "full"
  if (delivered <= 0) return "none"
  return "partial"
}

function matchesLineSearch(row, query) {
  const q = query.trim().toLowerCase()
  if (!q) return true
  const hay = [row.category_name, row.product_name, row.sku_code, row.barcode]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
  return hay.includes(q)
}

function filterLines(rows, filter, query) {
  return rows.filter((r) => {
    if (filter === "pending" && r.persisted) return false
    if (filter === "registered" && !r.persisted) return false
    return matchesLineSearch(r, query)
  })
}

function lineProgress(rows) {
  const total = rows.length
  const registered = rows.filter((r) => r.persisted).length
  return { total, registered, pending: Math.max(0, total - registered) }
}

describe("delivery-lines-ui", () => {
  const rows = [
    {
      source_detail_id: 1,
      product_name: "Leche",
      sku_code: "SKU-A",
      barcode: "78001",
      planned: 10,
      delivered: 10,
      persisted: false,
    },
    {
      source_detail_id: 2,
      product_name: "Leche",
      sku_code: "SKU-A",
      barcode: "78002",
      planned: 5,
      delivered: 3,
      persisted: true,
    },
    {
      source_detail_id: 3,
      product_name: "Pan",
      sku_code: "SKU-B",
      barcode: null,
      planned: 2,
      delivered: 0,
      persisted: true,
    },
  ]

  it("no marca registrada solo por cantidad precargada", () => {
    assert.equal(lineOutcome(rows[0]), null)
    assert.equal(lineProgress(rows).registered, 2)
    assert.equal(lineProgress(rows).pending, 1)
  })

  it("distingue mismo SKU por source_detail_id", () => {
    const pending = filterLines(rows, "pending", "SKU-A")
    assert.equal(pending.length, 1)
    assert.equal(pending[0].source_detail_id, 1)
    const registered = filterLines(rows, "registered", "SKU-A")
    assert.equal(registered.length, 1)
    assert.equal(registered[0].source_detail_id, 2)
  })

  it("busca por nombre, sku y barcode", () => {
    assert.equal(filterLines(rows, "all", "pan").length, 1)
    assert.equal(filterLines(rows, "all", "78001").length, 1)
    assert.equal(filterLines(rows, "all", "sku-b").length, 1)
  })

  it("outcomes parcial / no entregada", () => {
    assert.equal(lineOutcome(rows[1]), "partial")
    assert.equal(lineOutcome(rows[2]), "none")
  })

  it("escala a 100 líneas en progreso", () => {
    const many = Array.from({ length: 100 }, (_, i) => ({
      source_detail_id: i + 1,
      product_name: `P${i}`,
      sku_code: `S${i}`,
      barcode: null,
      planned: 1,
      delivered: 1,
      persisted: i < 35,
    }))
    const p = lineProgress(many)
    assert.equal(p.registered, 35)
    assert.equal(p.pending, 65)
    assert.equal(filterLines(many, "pending", "").length, 65)
  })
})
