import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import { detailLooksComplete } from "./offline-store.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")
const clientesSrc = readFileSync(
  join(root, "app/(app)/despachos/[planId]/clientes/page.tsx"),
  "utf8",
)
const appLinkSrc = readFileSync(join(root, "components/app-link.tsx"), "utf8")

describe("offline navigation + sync integrity", () => {
  it("SW no sirve text genérico a RSC (rompe soft-nav)", () => {
    assert.match(sw, /X-LQ-Offline-RSC/)
    assert.match(sw, /Offline RSC/)
    // El catch de RSC no debe caer a syntheticOfflineText como respuesta Flight.
    const rscBlock = sw.slice(sw.indexOf("if (isRsc)"), sw.indexOf("event.respondWith(handleAsset"))
    assert.doesNotMatch(rscBlock, /syntheticOfflineText\(\)/)
  })

  it("AppLink fuerza navegación documento cuando offline", () => {
    assert.match(appLinkSrc, /isDeviceOffline/)
    assert.match(appLinkSrc, /location\.assign/)
    assert.match(appLinkSrc, /preventDefault/)
  })

  it("Clientes no await syncPlanForOffline antes de liberar loading", () => {
    assert.match(clientesSrc, /setLoading\(false\)/)
    assert.match(clientesSrc, /void syncPlanForOffline/)
    assert.doesNotMatch(
      clientesSrc,
      /await syncPlanForOffline\([\s\S]*setLoading\(false\)/,
    )
  })

  it("detailLooksComplete exige client + documents[]", () => {
    assert.equal(detailLooksComplete(null), false)
    assert.equal(detailLooksComplete({ client: { name: "A" } }), false)
    assert.equal(
      detailLooksComplete({ client: { name: "A" }, documents: [] }),
      true,
    )
    assert.equal(
      detailLooksComplete({
        client: { name: "A" },
        documents: [{ products: [{ unidades: 2 }] }],
      }),
      true,
    )
  })
})
