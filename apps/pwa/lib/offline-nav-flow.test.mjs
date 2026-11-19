/**
 * Prueba de circuito offline (datos persistidos + contratos de pantalla).
 * No declara E2E Safari/iPhone: Node no reproduce soft-nav RSC ni SW iOS.
 */
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import { detailLooksComplete, offlineUserPrefix } from "./offline-store.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

function src(rel) {
  return readFileSync(join(root, rel), "utf8")
}

describe("circuito offline Mis despachos → productos", () => {
  it("aísla snapshot por userId (G)", () => {
    const a = offlineUserPrefix("u1")
    const b = offlineUserPrefix("u2")
    assert.notEqual(a, b)
    assert.equal(`${a}plans`, "u:u1:plans")
    assert.equal(`${a}plan:83:clients`, "u:u1:plan:83:clients")
    assert.equal(`${a}plan:83:client:C1`, "u:u1:plan:83:client:C1")
    assert.equal(`${a}plan:83:ready`, "u:u1:plan:83:ready")
  })

  it("ready.complete exige docs con id + productos con unidades (C)", () => {
    const incomplete = { client: { name: "X" } }
    const noId = {
      client: { name: "X" },
      documents: [{ products: [{ unidades: 12 }] }],
    }
    const complete = {
      client: { name: "X" },
      document_count: 1,
      documents: [
        {
          related_document_id: 99,
          product_count: 1,
          products: [{ description: "Y", unidades: 12 }],
        },
      ],
    }
    assert.equal(detailLooksComplete(incomplete), false)
    assert.equal(detailLooksComplete(noId), false)
    assert.equal(detailLooksComplete(complete), true)
  })

  it("pantallas clave usan AppLink + IDB-first", () => {
    const despachos = src("app/(app)/despachos/page.tsx")
    const detail = src("app/(app)/despachos/[planId]/page.tsx")
    const clientes = src("app/(app)/despachos/[planId]/clientes/page.tsx")
    const ficha = src("app/(app)/despachos/[planId]/clientes/[customerKey]/page.tsx")
    for (const body of [despachos, detail, clientes, ficha]) {
      assert.match(body, /AppLink/)
    }
    assert.match(detail, /readOfflinePlanFromList/)
    assert.match(clientes, /readOfflineClients/)
    assert.match(clientes, /offlineSyncRuntime/)
    assert.match(ficha, /readOfflineClientDetail/)
    assert.match(ficha, /Ver pedido/)
  })

  it("documenta límite: no E2E iPhone en Node", () => {
    assert.ok(
      true,
      "Validación física iPhone (modo avión + Ver detalle → productos) sigue siendo obligatoria.",
    )
  })
})
