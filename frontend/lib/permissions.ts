/** Permisos centralizados del ERP — no comparar roles como strings en componentes. */

export const MANAGEMENT_ROLES = new Set(["gerencia", "adm", "admin"])

export const ADMIN_ROLES = new Set([
  "adm",
  "admin",
  "superadmin",
  "super_admin",
  "administrator",
])

const MARGIN_VIEW_EXTRA_ROLES = new Set(["finanzas", "finance"])

export type StaffUserLike = {
  role?: string | null
  email?: string | null
  management_access?: boolean | null
  admin_access?: boolean | null
  permissions?: Partial<ErpPermissions> | null
}

export type ErpPermissions = {
  commercial_validation: boolean
  diagnostics: boolean
  costs: boolean
  margins: boolean
}

export function normalizedRole(role: string | null | undefined): string {
  return role?.trim().toLowerCase() ?? ""
}

export function roleFromUser(user: StaffUserLike | string | null | undefined): string {
  if (typeof user === "string") return normalizedRole(user)
  return normalizedRole(user?.role)
}

export function hasManagementAccess(
  user: StaffUserLike | string | null | undefined,
): boolean {
  if (user && typeof user !== "string" && user.management_access === true) {
    return true
  }
  const role = roleFromUser(user)
  return MANAGEMENT_ROLES.has(role) || ADMIN_ROLES.has(role)
}

export function hasAdminAccess(
  user: StaffUserLike | string | null | undefined,
): boolean {
  if (user && typeof user !== "string" && user.admin_access === true) {
    return true
  }
  return ADMIN_ROLES.has(roleFromUser(user))
}

export function hasMarginViewAccess(
  user: StaffUserLike | string | null | undefined,
): boolean {
  if (user && typeof user !== "string" && user.permissions?.margins === true) {
    return true
  }
  const role = roleFromUser(user)
  return hasManagementAccess(user) || MARGIN_VIEW_EXTRA_ROLES.has(role)
}

/** Futuro: módulos operacionales. */
export function hasOperationalAccess(
  user: StaffUserLike | string | null | undefined,
): boolean {
  return Boolean(roleFromUser(user) || (typeof user !== "string" && user?.email))
}

/** Futuro: módulos comerciales de venta. */
export function hasSalesAccess(
  user: StaffUserLike | string | null | undefined,
): boolean {
  return hasOperationalAccess(user)
}

export function resolveErpPermissions(
  user: StaffUserLike | string | null | undefined,
): ErpPermissions {
  if (user && typeof user !== "string" && user.permissions) {
    const p = user.permissions
    return {
      commercial_validation: Boolean(p.commercial_validation),
      diagnostics: Boolean(p.diagnostics),
      costs: Boolean(p.costs),
      margins: Boolean(p.margins),
    }
  }
  const mgmt = hasManagementAccess(user)
  return {
    commercial_validation: mgmt,
    diagnostics: mgmt,
    costs: mgmt,
    margins: hasMarginViewAccess(user),
  }
}

export function staffUserFromLocalStorage(): StaffUserLike {
  if (typeof window === "undefined") return {}
  return {
    role: localStorage.getItem("role"),
    email: localStorage.getItem("email"),
  }
}

export function canAccessCommercialValidation(
  user: StaffUserLike | string | null | undefined,
): boolean {
  return resolveErpPermissions(user).commercial_validation
}

export function canAccessDiagnostics(
  user: StaffUserLike | string | null | undefined,
): boolean {
  return resolveErpPermissions(user).diagnostics
}
