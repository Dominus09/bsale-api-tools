/**
 * Arquitectura de apariencia La Quillotana (Premium 4.0).
 *
 * Estado actual deliberado:
 * - `light` (Graphite claro) es la única experiencia activa y estable.
 * - `dark` y `system` están definidos como preferencia, pero NO se aplican:
 *   existen páginas legacy que aún no están validadas en oscuro y activar la
 *   clase `.dark` global dejaría pantallas mezcladas e ilegibles.
 *
 * Cuando un área completa esté adaptada y verificada, la activación consistirá
 * en resolver la preferencia aquí (con `prefers-color-scheme` para `system`) y
 * aplicar la clase al contenedor de esa área, nunca a ciegas en toda la ERP.
 */
export type AppearancePreference = "light" | "dark" | "system"

export const APPEARANCE_STORAGE_KEY = "lq-appearance"

/** Preferencia vigente con la que realmente se renderiza hoy. */
export const ACTIVE_APPEARANCE: AppearancePreference = "light"

export function readAppearancePreference(): AppearancePreference {
  if (typeof window === "undefined") return ACTIVE_APPEARANCE
  const stored = window.localStorage.getItem(APPEARANCE_STORAGE_KEY)
  return stored === "dark" || stored === "system" || stored === "light" ? stored : ACTIVE_APPEARANCE
}

export function storeAppearancePreference(preference: AppearancePreference): void {
  if (typeof window === "undefined") return
  window.localStorage.setItem(APPEARANCE_STORAGE_KEY, preference)
}

/** true cuando el modo oscuro esté habilitado para el área actual. Hoy: nunca. */
export function isDarkAppearanceEnabled(): boolean {
  return false
}
