import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { sumUniqueDocumentTotals } from "./picking-amount.ts"

describe("sumUniqueDocumentTotals", () => {
  it("suma una vez por related_document_id", () => {
    const total = sumUniqueDocumentTotals([
      { related_document_id: 10, document_total: 1000 },
      { related_document_id: 10, document_total: 1000 },
      { related_document_id: 11, document_total: 500.5 },
    ])
    assert.equal(total, 1500.5)
  })

  it("null si no hay montos", () => {
    assert.equal(sumUniqueDocumentTotals([{ related_document_id: 1 }]), null)
  })
})
