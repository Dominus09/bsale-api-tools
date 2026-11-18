import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { buildResultadoSummary } from "./resultado-summary.ts"

describe("buildResultadoSummary partial closed visit", () => {
  it("resume docs, líneas, cobranza y observaciones sin exigir firma", () => {
    const s = buildResultadoSummary({
      docs: [
        {
          document_type: "Factura",
          document_number: 9001,
          result_status: "partial",
          document_total: 155033,
        },
      ],
      lines: [
        { product_name: "A", planned_units: 4, delivered_units: 4 },
        { product_name: "B", planned_units: 2, delivered_units: 1 },
      ],
      collections: [
        { method: "cash", declared_amount: 50000, document_result_id: 1 },
        { method: "pending", declared_amount: 15033, document_result_id: 1 },
      ],
      deliveryObservations: "Quedaron 2 cajas pendientes",
      conformity: null,
    })
    assert.equal(s.docCount, 1)
    assert.equal(s.lineCount, 2)
    assert.equal(s.collectionCount, 2)
    assert.equal(s.hasConformity, false)
    assert.equal(s.conformityLabel, null)
    assert.match(s.deliveryObservations || "", /2 cajas/)
    assert.ok(s.compactLines.some((l) => /parciales/.test(l)))
    assert.ok(s.compactLines.some((l) => /Observaciones de entrega/.test(l)))
    assert.equal(s.compactLines.some((l) => /Conformidad:/.test(l)), false)
  })

  it("conserva firma histórica cuando existe evidencia real", () => {
    const s = buildResultadoSummary({
      docs: [],
      lines: [],
      collections: [],
      conformity: {
        receptor_name: "Carlos",
        refused: false,
        invalidated_at: null,
        signed_at: "2026-01-01T12:00:00Z",
        signature_object_key: "sig/k",
      },
    })
    assert.equal(s.hasConformity, true)
    assert.match(s.conformityLabel || "", /Carlos/)
  })
})
