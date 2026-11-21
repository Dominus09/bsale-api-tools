import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

describe("offline-v2 pilot auth offline grants", () => {
  it("persiste grants en IndexedDB (no localStorage de permisos)", () => {
    const auth = readFileSync(join(root, "lib/offline-v2-pilot-auth.ts"), "utf8")
    const store = readFileSync(join(root, "lib/offline-store.ts"), "utf8")
    assert.match(store, /OfflineV2PlanGrant/)
    assert.match(store, /v2grant/)
    assert.match(auth, /hydrateOfflineV2AuthFromIdb/)
    assert.match(auth, /resolveOfflineV2UiEnabledForPlan/)
    assert.match(auth, /PILOT_FETCH_TIMEOUT_MS/)
    assert.match(auth, /isDeviceOffline/)
    assert.doesNotMatch(auth, /localStorage\.setItem\([^)]*grant/i)
  })

  it("ficha y clientes no bloquean con await de pilot-status offline", () => {
    const ficha = readFileSync(
      join(
        root,
        "app/(app)/despachos/[planId]/clientes/[customerKey]/page.tsx",
      ),
      "utf8",
    )
    const clientes = readFileSync(
      join(root, "app/(app)/despachos/[planId]/clientes/page.tsx"),
      "utf8",
    )
    assert.match(ficha, /resolveOfflineV2UiEnabledForPlan/)
    assert.match(ficha, /hasLocal/)
    assert.match(clientes, /IDB-FIRST|hasLocal/)
    assert.match(clientes, /revalidar en background|background/)
  })

  it("páginas de registro local no exigen refresh de red", () => {
    const completa = readFileSync(
      join(
        root,
        "app/(app)/despachos/[planId]/clientes/[customerKey]/entrega/offline-completa/page.tsx",
      ),
      "utf8",
    )
    assert.match(completa, /resolveOfflineV2UiEnabledForPlan/)
    assert.doesNotMatch(completa, /await refreshOfflineV2PilotAuthorization/)
  })
})
