import assert from "node:assert/strict"
import { describe, it } from "node:test"

import {
  clientSelectionKey,
  encodeBatchRoleDescription,
  priorityReviewNeeded,
  sortLoadOrgClients,
  stripBatchRoleDescription,
} from "./load-organization-ui.ts"

describe("load-organization-ui", () => {
  it("sorts by venta, sku, cajas and peso without mutating source", () => {
    const clients = [
      { client_name: "B", venta_oc_clp: 100, sku_distinct: 10, cajas_equivalentes: 2, peso_conocido_kg: 5 },
      { client_name: "A", venta_oc_clp: 500, sku_distinct: 3, cajas_equivalentes: 20, peso_conocido_kg: 1 },
      { client_name: "C", venta_oc_clp: 200, sku_distinct: 40, cajas_equivalentes: null, peso_conocido_kg: 50 },
    ]
    const byVenta = sortLoadOrgClients(clients, "venta_desc")
    assert.equal(byVenta[0].client_name, "A")
    const bySku = sortLoadOrgClients(clients, "sku_desc")
    assert.equal(bySku[0].client_name, "C")
    const byCajas = sortLoadOrgClients(clients, "cajas_desc")
    assert.equal(byCajas[0].client_name, "A")
    const byPeso = sortLoadOrgClients(clients, "peso_desc")
    assert.equal(byPeso[0].client_name, "C")
    assert.equal(clients[0].client_name, "B")
  })

  it("encodes role without SQL fields and detects priority review", () => {
    assert.equal(encodeBatchRoleDescription("priority"), "role=priority")
    assert.equal(stripBatchRoleDescription("role=consolidated\nresto"), "resto")
    assert.equal(clientSelectionKey({ client_id: 5, client_name: "X" }), "id:5")
    assert.equal(priorityReviewNeeded("priority", 3), true)
    assert.equal(priorityReviewNeeded("priority", 2), false)
    assert.equal(priorityReviewNeeded("consolidated", 15), false)
  })
})
