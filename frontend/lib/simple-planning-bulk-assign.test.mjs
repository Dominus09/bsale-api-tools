/**
 * Ejecutar: node --test frontend/lib/simple-planning-bulk-assign.test.mjs
 * Réplica mínima alineada con simple-planning-bulk-assign.ts
 */

import assert from "node:assert/strict"
import { describe, it } from "node:test"

function folioKey(folio) {
  return String(folio)
}

function toggleFolioInSelection(selected, folio, checked) {
  const next = new Set(selected)
  const key = folioKey(folio)
  if (checked) next.add(key)
  else next.delete(key)
  return next
}

function selectAllFolios(folios) {
  return new Set(folios.map(folioKey))
}

function clearFolioSelection() {
  return new Set()
}

function applyBulkTruckAssignment(assignmentByFolio, selectedFolios, truckId) {
  const next = { ...assignmentByFolio }
  for (const folio of selectedFolios) {
    const key = folioKey(folio)
    if (truckId == null) delete next[key]
    else next[key] = truckId
  }
  return next
}

function uniqueMunicipalities(items) {
  const set = new Set()
  for (const item of items) {
    const m = (item.client?.municipality || "").trim()
    if (m) set.add(m)
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
}

function filterByMunicipality(items, municipality) {
  if (!municipality || municipality === "__all__") return items
  return items.filter((i) => (i.client?.municipality || "").trim() === municipality)
}

function visibleAssignableFolios(items) {
  const out = []
  const seen = new Set()
  for (const item of items) {
    const folio = Number(item.folio)
    if (!Number.isFinite(folio) || seen.has(folio)) continue
    if (item.blocking) continue
    if (
      item.status === "not_found" ||
      item.status === "invalid_folio" ||
      item.status === "duplicate_in_list" ||
      item.status === "cancelled_or_replaced"
    ) {
      continue
    }
    seen.add(folio)
    out.push(folio)
  }
  return out
}

describe("simple-planning bulk assign", () => {
  it("toggles individual and select-all visible", () => {
    let sel = clearFolioSelection()
    sel = toggleFolioInSelection(sel, 10, true)
    sel = toggleFolioInSelection(sel, 11, true)
    assert.equal(sel.size, 2)
    sel = toggleFolioInSelection(sel, 10, false)
    assert.equal(sel.has("10"), false)
    sel = selectAllFolios([10, 11, 12])
    assert.equal(sel.size, 3)
  })

  it("filters by municipality", () => {
    const items = [
      { folio: 1, client: { municipality: "Castro" } },
      { folio: 2, client: { municipality: "Ancud" } },
      { folio: 3, client: { municipality: "Castro" } },
      { folio: 4, client: { municipality: null } },
    ]
    assert.deepEqual(uniqueMunicipalities(items), ["Ancud", "Castro"])
    assert.equal(filterByMunicipality(items, "Castro").length, 2)
    assert.equal(filterByMunicipality(items, "__all__").length, 4)
  })

  it("bulk assigns and reassigns without duplicating", () => {
    let map = applyBulkTruckAssignment({}, [1, 2, 3], 5)
    assert.deepEqual(map, { 1: 5, 2: 5, 3: 5 })
    map = applyBulkTruckAssignment(map, [2, 3], 8)
    assert.deepEqual(map, { 1: 5, 2: 8, 3: 8 })
    map = applyBulkTruckAssignment(map, [1], null)
    assert.deepEqual(map, { 2: 8, 3: 8 })
  })

  it("supports up to 200 OC and skips blocked", () => {
    const items = []
    for (let i = 1; i <= 200; i++) {
      items.push({ folio: i, blocking: false, status: "found_available" })
    }
    items.push({ folio: 201, blocking: true, status: "not_found" })
    const folios = visibleAssignableFolios(items)
    assert.equal(folios.length, 200)
  })

  it("individual override after bulk", () => {
    let map = applyBulkTruckAssignment({}, [1, 2], 3)
    map = applyBulkTruckAssignment(map, [2], 9)
    assert.equal(map["1"], 3)
    assert.equal(map["2"], 9)
  })
})
