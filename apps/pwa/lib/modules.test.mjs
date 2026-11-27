import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { describe, it } from "node:test"
import { PWA_MODULES, modulesForProfiles, hasAnyPwaModule, resolvePostLoginPath } from "./modules.ts"
import { BRAND } from "./brand.ts"

describe("selector general de Quillotana Operativa", () => {
  it("login de chofer único no salta el selector", () => {
    assert.equal(resolvePostLoginPath(["chofer"]), "/modulos")
  })
  it("login con perfiles múltiples no salta el selector", () => {
    assert.equal(resolvePostLoginPath(["chofer", "vendedor", "bodega", "supervisor"]), "/modulos")
    assert.equal(resolvePostLoginPath(["chofer", "vendedor"]), "/modulos")
  })
  it("cada perfil no operativo aún entra al selector", () => {
    for (const profile of ["vendedor", "bodega", "supervisor"]) {
      assert.equal(resolvePostLoginPath([profile]), "/modulos")
    }
  })
  it("sin perfiles operativos no entra", () => {
    assert.equal(resolvePostLoginPath([]), "/login?denied=profile")
    assert.equal(resolvePostLoginPath(["admin", "unknown"]), "/login?denied=profile")
    assert.equal(hasAnyPwaModule(null), false)
  })
  it("solo muestra perfiles autorizados sin duplicar módulos", () => {
    assert.deepEqual(modulesForProfiles([" VENDEDOR ", "vendedor", "unknown"]).map(m => m.id), ["vendedores"])
    assert.deepEqual(modulesForProfiles(["chofer", "supervisor"]).map(m => m.id), ["choferes", "supervision"])
  })
  it("mantiene el estado real de cada módulo y las rutas existentes", () => {
    assert.deepEqual(PWA_MODULES.map(m => [m.id, m.status, m.href]), [
      ["choferes", "ready", "/despachos"],
      ["vendedores", "preparing", "/vendedores"],
      ["bodega", "soon", "/bodega"],
      ["supervision", "soon", "/supervision"],
    ])
  })
  it("identidad general sin modificar la marca", () => {
    assert.equal(BRAND.name, "Quillotana Operativa")
    assert.equal(BRAND.subtitle, "Gestión integral de operaciones")
  })
  it("selector no auto-redirige y conserva navegación offline existente", () => {
    const picker = readFileSync(new URL("../app/(app)/modulos/page.tsx", import.meta.url), "utf8")
    assert.doesNotMatch(picker, /router\.(?:replace|push)\(/)
    assert.match(picker, /modulesForProfiles/)
    assert.match(picker, /<AppLink/)
    assert.match(picker, /status !== 401 && status !== 403 && isLikelyOfflineError/)
    const manifest = readFileSync(new URL("../app/manifest.ts", import.meta.url), "utf8")
    assert.match(manifest, /id: "\/despachos"/)
    assert.match(manifest, /start_url: "\/despachos"/)
  })
})
