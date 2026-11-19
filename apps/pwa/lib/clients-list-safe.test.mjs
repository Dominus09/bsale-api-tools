import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  extractNextStaticAssets,
  normalizeClientCards,
} from "./clients-list-safe.ts"

describe("clients-list-safe", () => {
  it("omite filas sin customer_key (snapshot corrupto)", () => {
    const { items, skipped } = normalizeClientCards([
      null,
      { client_name: "Sin key" },
      { customer_key: "id:1", client_name: "OK", document_count: 2 },
      "x",
    ])
    assert.equal(items.length, 1)
    assert.equal(items[0].customer_key, "id:1")
    assert.equal(items[0].document_count, 2)
    assert.equal(skipped, 3)
  })

  it("extrae chunks /_next/static del HTML de ruta", () => {
    const html = `<!doctype html><script src="/_next/static/chunks/app/despachos/clientes/page-abc.js"></script>
<link rel="stylesheet" href="/_next/static/css/app.css"/>
<script src="https://cdn.example/x.js"></script>`
    const assets = extractNextStaticAssets(html)
    assert.deepEqual(assets.sort(), [
      "/_next/static/chunks/app/despachos/clientes/page-abc.js",
      "/_next/static/css/app.css",
    ])
  })
})
