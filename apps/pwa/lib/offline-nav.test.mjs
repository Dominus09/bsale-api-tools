import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import {
  detailLooksComplete,
  planSnapshotIntegrity,
} from "./offline-store.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const sw = readFileSync(join(root, "public", "sw.js"), "utf8")
const clientesSrc = readFileSync(
  join(root, "app/(app)/despachos/[planId]/clientes/page.tsx"),
  "utf8",
)
const appLinkSrc = readFileSync(join(root, "components/app-link.tsx"), "utf8")
const syncSrc = readFileSync(join(root, "lib/offline-sync.ts"), "utf8")

describe("offline navigation + sync integrity", () => {
  it("SW no sirve text genérico a RSC (rompe soft-nav)", () => {
    assert.match(sw, /X-LQ-Offline-RSC/)
    assert.match(sw, /Offline RSC/)
    const rscBlock = sw.slice(sw.indexOf("if (isRsc)"), sw.indexOf("event.respondWith(handleAsset"))
    assert.doesNotMatch(rscBlock, /syntheticOfflineText\(\)/)
  })

  it("SW cachea HTML /despachos en fetch no-navigate (warm)", () => {
    assert.match(sw, /lq-pwa-v7/)
    assert.match(sw, /text\/html/)
    assert.match(sw, /PAGE_CACHE/)
    assert.match(sw, /path\.startsWith\("\/despachos"\)/)
  })

  it("RSC online usa fetch primero; 503 solo en catch", () => {
    const rscBlock = sw.slice(sw.indexOf("if (isRsc)"), sw.indexOf("event.respondWith(handleAsset"))
    assert.match(rscBlock, /return await fetch\(request\)/)
    assert.match(rscBlock, /catch/)
    assert.match(rscBlock, /status: 503/)
  })

  it("SW no intercepta API ni login", () => {
    assert.match(sw, /isApiPath/)
    assert.match(sw, /api-upstream/)
    assert.match(sw, /\/login/)
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

  it("sync await warmPlanRoutes antes de complete", () => {
    assert.match(syncSrc, /await warmPlanRoutes/)
    assert.match(syncSrc, /planSnapshotIntegrity/)
  })

  it("detailLooksComplete exige ids, products y unidades", () => {
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
      false,
      "sin related_document_id ni document_number",
    )
    assert.equal(
      detailLooksComplete({
        client: { name: "A" },
        document_count: 1,
        documents: [
          {
            related_document_id: 10,
            product_count: 1,
            products: [{ description: "x" }],
          },
        ],
      }),
      false,
      "sin unidades",
    )
    assert.equal(
      detailLooksComplete(
        {
          client: { name: "A" },
          document_count: 1,
          documents: [
            {
              related_document_id: 10,
              product_count: 1,
              products: [{ description: "x", unidades: 12 }],
            },
          ],
        },
        { document_count: 1, units_total: 12 },
      ),
      true,
    )
  })

  it("planSnapshotIntegrity valida 8 clientes / 11 docs (PLAN-00083)", () => {
    const clients = Array.from({ length: 8 }, (_, i) => ({
      document_count: i < 3 ? 2 : 1,
      units_total: 10,
    }))
    // 2+2+2+1+1+1+1+1 = 11
    assert.equal(
      clients.reduce((a, c) => a + c.document_count, 0),
      11,
    )
    const ok = planSnapshotIntegrity({
      clients,
      summary: { clients: 8, documents: 11 },
      detailsOk: 8,
      documentsFromDetails: 11,
      truncated: false,
    })
    assert.equal(ok.ok, true)
    assert.equal(ok.expectedClients, 8)
    assert.equal(ok.expectedDocuments, 11)

    const badDocs = planSnapshotIntegrity({
      clients,
      summary: { clients: 8, documents: 11 },
      detailsOk: 8,
      documentsFromDetails: 10,
      truncated: false,
    })
    assert.equal(badDocs.ok, false)
  })
})
