import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

describe("offline-v2 lab route isolation", () => {
  it("middleware bloquea lab sin OFFLINE_V2_LAB_ENABLED y en *.quillotana.cl", () => {
    const src = readFileSync(join(root, "middleware.ts"), "utf8")
    assert.match(src, /OFFLINE_V2_LAB_ENABLED/)
    assert.match(src, /quillotana\.cl/)
    assert.match(src, /status: 404/)
    assert.match(src, /pruebas\/offline-v2-lab/)
  })

  it("página lab documenta no-prod y bloquea hosts Quillotana", () => {
    const src = readFileSync(
      join(root, "app/pruebas/offline-v2-lab/page.tsx"),
      "utf8",
    )
    assert.match(src, /isProductionQuillotanaHost/)
    assert.match(src, /nunca api\.quillotana\.cl|No escribe a api\.quillotana\.cl/i)
  })
})
