import { hasMarginViewAccess, type StaffUserLike } from "@/lib/permissions"

/**
 * Empresas con BSALE RAW completo y Analítica V2 disponible.
 * Debe coincidir con `RAW_READY_COMPANY_IDS` del backend
 * (backend/services/analytics_v2/contracts.py).
 */
export const ANALYTICS_V2_COMPANY_IDS: ReadonlySet<number> = new Set([3])

export const ANALYTICS_V2_DASHBOARD_PATH = "/analitica-v2/dashboard"
export const LEGACY_DASHBOARD_PATH = "/dashboard"

/**
 * Inicio principal tras el login o al elegir empresa:
 * - empresa con Analítica V2 y usuario con permiso de márgenes (el mismo que
 *   exige el endpoint del Dashboard V2) → Dashboard comercial V2;
 * - cualquier otro caso → dashboard actual, sin forzar rutas que darían 403/409.
 */
export function preferredHomePath(
  companyId: number | null | undefined,
  user: StaffUserLike | string | null | undefined,
): string {
  if (companyId != null && ANALYTICS_V2_COMPANY_IDS.has(companyId) && hasMarginViewAccess(user)) {
    return ANALYTICS_V2_DASHBOARD_PATH
  }
  return LEGACY_DASHBOARD_PATH
}
