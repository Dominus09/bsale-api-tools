import assert from "node:assert/strict"
import { describe, it } from "node:test"

function moneyCLP(n) {
  if (n == null || Number.isNaN(n)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

function docKeyFromDocument(doc) {
  const id = doc.related_document_id || doc.document_number
  return id == null || id === "" ? "" : String(id)
}

function buildEntregaHref(planId, customerKey, step, extra) {
  const enc = encodeURIComponent(customerKey)
  const base = `/despachos/${planId}/clientes/${enc}/entrega`
  if (step === "ficha") return `/despachos/${planId}/clientes/${enc}`
  if (step == null) return base
  const sub = typeof step === "number"
    ? ["documentos", "cobranza", "firma"][step - 1]
    : step
  let path = `${base}/${sub}`
  if (sub === "documentos" && extra?.docKey) {
    path += `/${encodeURIComponent(extra.docKey)}`
  }
  return path
}

describe("delivery-flow helpers", () => {
  it("moneyCLP formatea CLP", () => {
    assert.match(moneyCLP(155033), /155/)
  })

  it("docKey prioriza related_document_id", () => {
    assert.equal(docKeyFromDocument({ related_document_id: 9, document_number: 1 }), "9")
    assert.equal(docKeyFromDocument({ document_number: 51819 }), "51819")
  })

  it("buildEntregaHref rutas modulares", () => {
    assert.equal(
      buildEntregaHref(12, "C-1", "documentos"),
      "/despachos/12/clientes/C-1/entrega/documentos",
    )
    assert.equal(
      buildEntregaHref(12, "C-1", "documentos", { docKey: "99" }),
      "/despachos/12/clientes/C-1/entrega/documentos/99",
    )
    assert.equal(buildEntregaHref(12, "C-1", "ficha"), "/despachos/12/clientes/C-1")
  })
})
