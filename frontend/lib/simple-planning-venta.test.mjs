import assert from "node:assert/strict"
import { describe, it } from "node:test"

import {
  draftTruckStatsFromAssignments,
  sumOcVentaClp,
  uniqueClientCount,
} from "./simple-planning-folios.ts"

describe("sumOcVentaClp", () => {
  it("sums OC amounts without inventing values", () => {
    assert.equal(
      sumOcVentaClp([
        { total_amount: 1000 },
        { total_amount: 250.5 },
        { total_amount: null },
        {},
      ]),
      1250.5,
    )
    assert.equal(sumOcVentaClp([]), 0)
  })
})

describe("draftTruckStatsFromAssignments", () => {
  it("groups by truck and counts clients without double-counting multi-OC", () => {
    const items = [
      {
        folio: 1,
        total_amount: 100,
        client: { client_id: 10, client_name: "A" },
        weight: { known_weight_kg: 5, coverage_percent: 100 },
      },
      {
        folio: 2,
        total_amount: 50,
        client: { client_id: 10, client_name: "A" },
        weight: { known_weight_kg: 2, coverage_percent: 80 },
      },
      {
        folio: 3,
        total_amount: 200,
        client: { client_id: 20, client_name: "B" },
        weight: { known_weight_kg: 10, coverage_percent: 100 },
      },
    ]
    const stats = draftTruckStatsFromAssignments(
      items,
      { 1: 7, 2: 7, 3: 8 },
      uniqueClientCount,
    )
    assert.equal(stats.length, 2)
    const t7 = stats.find((s) => s.truckId === 7)
    assert.equal(t7.ocCount, 2)
    assert.equal(t7.clients, 1)
    assert.equal(t7.ventaOcClp, 150)
    assert.equal(t7.knownKg, 7)
    const t8 = stats.find((s) => s.truckId === 8)
    assert.equal(t8.ventaOcClp, 200)
    assert.equal(t8.clients, 1)
  })
})
