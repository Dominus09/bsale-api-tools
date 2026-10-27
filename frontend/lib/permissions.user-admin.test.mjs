/**
 * Permisos UI Usuarios y accesos: solo flags de /auth/me.
 * Ejecutar: node --test frontend/lib/permissions.user-admin.test.mjs
 *
 * Nota: este archivo reimplementa las reglas de can*UserAdmin para no depender
 * de resolución TS/Next; mantener alineado con permissions.ts.
 */

import assert from "node:assert/strict"
import { describe, it } from "node:test"

function canViewUserAdmin(user) {
  if (!user || typeof user === "string") return false
  return user.permissions?.user_admin_view === true
}

function canMutateUserAdmin(user) {
  if (!user || typeof user === "string") return false
  return user.permissions?.user_admin === true
}

function canAssignAdmRole(user) {
  if (!user || typeof user === "string") return false
  return user.permissions?.user_admin_assign_adm === true
}

describe("user admin UI permissions from /auth/me", () => {
  it("gerencia autorizada", () => {
    const u = {
      role: "gerencia",
      permissions: { user_admin_view: true, user_admin: true, user_admin_assign_adm: true },
    }
    assert.equal(canViewUserAdmin(u), true)
    assert.equal(canMutateUserAdmin(u), true)
    assert.equal(canAssignAdmRole(u), true)
  })

  it("gerencia sin autorización de mutación", () => {
    const u = {
      role: "gerencia",
      permissions: { user_admin_view: true, user_admin: false, user_admin_assign_adm: false },
    }
    assert.equal(canViewUserAdmin(u), true)
    assert.equal(canMutateUserAdmin(u), false)
    assert.equal(canAssignAdmRole(u), false)
  })

  it("adm autorizado", () => {
    const u = {
      role: "adm",
      permissions: { user_admin_view: true, user_admin: true, user_admin_assign_adm: false },
    }
    assert.equal(canMutateUserAdmin(u), true)
    assert.equal(canAssignAdmRole(u), false)
  })

  it("adm sin allowlist no muta aunque el rol sea adm", () => {
    const u = { role: "adm", permissions: { user_admin_view: true, user_admin: false } }
    assert.equal(canMutateUserAdmin(u), false)
  })

  it("operativo sin flags", () => {
    const u = { role: "operativo", permissions: { user_admin_view: false, user_admin: false } }
    assert.equal(canViewUserAdmin(u), false)
    assert.equal(canMutateUserAdmin(u), false)
  })

  it("sin /auth/me (null o sin permissions) no muestra acciones", () => {
    assert.equal(canViewUserAdmin(null), false)
    assert.equal(canMutateUserAdmin({ role: "adm" }), false)
    assert.equal(canMutateUserAdmin({ role: "adm", permissions: {} }), false)
  })

  it("no usa string role como autorización", () => {
    assert.equal(canViewUserAdmin("adm"), false)
    assert.equal(canMutateUserAdmin("gerencia"), false)
  })

  it("allowlist vacía: gerencia ve pero no muta (flags de /auth/me)", () => {
    const u = {
      role: "gerencia",
      permissions: {
        user_admin_view: true,
        user_admin: false,
        user_admin_assign_adm: false,
      },
    }
    assert.equal(canViewUserAdmin(u), true)
    assert.equal(canMutateUserAdmin(u), false)
    assert.equal(canAssignAdmRole(u), false)
  })

  it("is_owner solo desde flag de /auth/me", () => {
    function isOwnerUser(user) {
      if (!user || typeof user === "string") return false
      return user.is_owner === true
    }
    assert.equal(isOwnerUser({ role: "gerencia", is_owner: true }), true)
    assert.equal(isOwnerUser({ role: "gerencia" }), false)
    assert.equal(isOwnerUser("gerencia"), false)
  })
})
