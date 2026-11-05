import assert from "node:assert/strict"
import { describe, it } from "node:test"

import {
  filterPickingProductRows,
  paginatePickingProductRows,
  PICKING_PRODUCT_PAGE_SIZE,
  summarizePickingProductRows,
  uniquePickingProductCategories,
} from "./picking-product-table.ts"

const items = Array.from({ length: 40 }, (_, i) => ({
  sucursal_bodega: i % 2 === 0 ? "Bodega Central" : "Bodega 1",
  tipo_producto: i < 10 ? "BEBIDAS" : "ABARROTES",
  producto_variante: `Producto ${i + 1}`,
  display_name: `Producto ${i + 1}`,
  codigo_barras: `780${String(i).padStart(10, "0")}`,
  unidades: 10,
  total_monto: 1000,
  units_per_box: 10,
  cajas_efectivas: 1,
}))

describe("picking-product-table", () => {
  it("filters by query and category without mutating source", () => {
    const filtered = filterPickingProductRows(items, {
      query: "Producto 1",
      category: "BEBIDAS",
    })
    assert.ok(filtered.length >= 1)
    assert.equal(items.length, 40)
    assert.equal(uniquePickingProductCategories(items).includes("BEBIDAS"), true)
  })

  it("paginates 15 rows and reports range", () => {
    assert.equal(PICKING_PRODUCT_PAGE_SIZE, 15)
    const page1 = paginatePickingProductRows(items, 1)
    assert.equal(page1.rows.length, 15)
    assert.equal(page1.from, 1)
    assert.equal(page1.to, 15)
    assert.equal(page1.total, 40)
    const page3 = paginatePickingProductRows(items, 3)
    assert.equal(page3.rows.length, 10)
    assert.equal(page3.from, 31)
    assert.equal(page3.to, 40)
  })

  it("summarizes without inventing boxes", () => {
    const s = summarizePickingProductRows(items.slice(0, 2))
    assert.equal(s.units, 20)
    assert.equal(s.skus, 2)
  })
})
