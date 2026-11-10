import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  commercialProductLabel,
  productBarcodeLabel,
  productCategoryLabel,
} from "./delivery-labels.ts"

describe("commercialProductLabel P4.4", () => {
  it("combina producto + variante", () => {
    assert.equal(
      commercialProductLabel({
        product_name: "ROYAL GUARD",
        variant_name: "RETORNABLE 1.0 LT (SEC 12)",
        category_name: "CERVEZAS",
      }),
      "ROYAL GUARD — RETORNABLE 1.0 LT (SEC 12)",
    )
  })

  it("no duplica si la variante ya incluye el producto", () => {
    assert.equal(
      commercialProductLabel({
        product_name: "Cristal",
        variant_name: "Cristal retornable 970 cc",
      }),
      "Cristal retornable 970 cc",
    )
  })

  it("usa commercial_name como respaldo", () => {
    assert.equal(
      commercialProductLabel({
        commercial_name: "SOLO COMERCIAL",
      }),
      "SOLO COMERCIAL",
    )
  })

  it("expone categoría y barcode sin inventar", () => {
    assert.equal(productCategoryLabel({ category_name: "CERVEZAS" }), "CERVEZAS")
    assert.equal(productCategoryLabel({}), null)
    assert.equal(
      productBarcodeLabel({ codigo_barras: "7802100430007" }),
      "7802100430007",
    )
  })
})
