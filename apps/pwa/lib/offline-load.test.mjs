import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  offlineBannerLabel,
  plansBundleUsable,
  shouldUseOfflineSnapshot,
} from "./offline-store.ts"

describe("offline-load orchestration", () => {
  it("prioriza snapshot cuando navigator.onLine es false", () => {
    assert.equal(shouldUseOfflineSnapshot(false), true)
    assert.equal(shouldUseOfflineSnapshot(true), false)
    assert.equal(shouldUseOfflineSnapshot(true, { status: 503 }), true)
    assert.equal(shouldUseOfflineSnapshot(true, { status: 401 }), false)
  })

  it("considera usable un bundle con items", () => {
    assert.equal(plansBundleUsable(null), false)
    assert.equal(plansBundleUsable({ items: [], count: 0, syncedAt: "x" }), false)
    assert.equal(
      plansBundleUsable({
        items: [{ id: 1 }],
        count: 1,
        syncedAt: "2026-11-18T22:00:00.000Z",
      }),
      true,
    )
  })

  it("formatea banner único sin conexión", () => {
    assert.match(offlineBannerLabel(null), /sin datos/i)
    assert.match(offlineBannerLabel("2026-11-18T22:20:00.000Z"), /^Sin conexión · Datos del /)
  })
})

describe("causa raíz spinner infinito", () => {
  it("documenta que sin IDB-first un fetch colgado deja items=null", () => {
    let items = null
    let settled = false
    const hung = new Promise(() => {
      /* never settles — Safari offline API fetch */
    })
    void hung.then(() => {
      settled = true
      items = []
    })
    assert.equal(settled, false)
    assert.equal(items, null)
    const offlineFirst = { status: "success", items: [{ id: 79 }] }
    items = offlineFirst.items
    assert.equal(items.length, 1)
  })
})
