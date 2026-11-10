import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  filterActiveCollections,
  summarizeCollections,
} from "./active-collections.ts"

describe("cobranza vigente vs histórica", () => {
  it("excluye huérfanas y resume sin doble conteo", () => {
    const rows = [
      { id: 1, document_result_id: null, method: "pending", declared_amount: 120000 },
      { id: 2, document_result_id: 9, method: "cash", declared_amount: 50000 },
      {
        id: 3,
        document_result_id: 9,
        method: "transfer_declared",
        declared_amount: 90000,
      },
      { id: 4, document_result_id: 9, method: "pending", declared_amount: 15033 },
    ]
    const active = filterActiveCollections(rows, [9])
    assert.equal(active.length, 3)
    const s = summarizeCollections(active, 155033)
    assert.equal(s.cash, 50000)
    assert.equal(s.transferDeclared, 90000)
    assert.equal(s.pendingExplicit, 15033)
    assert.equal(s.pendingBalance, 15033)
    assert.equal(s.declaredAll, 155033)
  })
})
