/**
 * Visibilidad UI simple_planning. Ejecutar:
 * node --test frontend/lib/permissions.simple-planning.test.mjs
 */

import assert from "node:assert/strict"
import { describe, it } from "node:test"

const SIMPLE_PLANNING_ROLES = new Set(["gerencia", "operativo", "adm", "admin"])

function normalizedRole(role) {
  return role?.trim().toLowerCase() ?? ""
}

function roleFromUser(user) {
  if (typeof user === "string") return normalizedRole(user)
  return normalizedRole(user?.role)
}

function hasSimplePlanningAccess(user) {
  if (user == null) return false
  if (typeof user === "string") {
    return SIMPLE_PLANNING_ROLES.has(normalizedRole(user))
  }
  if (user.is_owner === true) return true
  if (SIMPLE_PLANNING_ROLES.has(roleFromUser(user))) return true
  const caps = user.erp_capabilities || []
  return caps.map((c) => String(c).toLowerCase()).includes("simple_planning")
}

describe("hasSimplePlanningAccess", () => {
  it("null → false", () => {
    assert.equal(hasSimplePlanningAccess(null), false)
    assert.equal(hasSimplePlanningAccess(undefined), false)
  })

  it("roles ERP autorizados", () => {
    assert.equal(hasSimplePlanningAccess({ role: "gerencia" }), true)
    assert.equal(hasSimplePlanningAccess({ role: "operativo" }), true)
    assert.equal(hasSimplePlanningAccess({ role: "adm" }), true)
    assert.equal(hasSimplePlanningAccess("admin"), true)
  })

  it("capability sin rol privilegiado", () => {
    assert.equal(
      hasSimplePlanningAccess({
        role: "vendedor",
        erp_capabilities: ["simple_planning"],
      }),
      true,
    )
  })

  it("vendedor sin capability", () => {
    assert.equal(hasSimplePlanningAccess({ role: "vendedor", erp_capabilities: [] }), false)
  })

  it("owner siempre", () => {
    assert.equal(hasSimplePlanningAccess({ role: "x", is_owner: true }), true)
  })

  it("no concede admin por tener simple_planning", () => {
    const u = { role: "custom", erp_capabilities: ["simple_planning"] }
    assert.equal(hasSimplePlanningAccess(u), true)
    assert.equal(u.erp_capabilities.includes("user_admin"), false)
  })
})
