/**
 * Apariencia La Quillotana — Graphite claro / Graphite Dark (Premium 4.1).
 *
 * Reglas de alcance (para nunca mostrar una interfaz mezclada o ilegible):
 * - La preferencia del usuario (claro/oscuro) se persiste en el navegador.
 * - El tema oscuro SOLO se aplica en las rutas adaptadas y verificadas de
 *   `DARK_THEME_ROUTE_PREFIXES`. En cualquier otra ruta la ERP se renderiza
 *   en claro aunque la preferencia sea oscura, y el selector luna/sol no se
 *   muestra.
 * - La clase `.dark` se aplica únicamente al contenedor del dashboard
 *   (no a <html>), de modo que los módulos legacy jamás heredan el tema.
 * - Al volver a una ruta adaptada, la preferencia oscura se reaplica sola.
 */
export type AppearanceTheme = "light" | "dark"

export const APPEARANCE_STORAGE_KEY = "lq-appearance"

export const DEFAULT_THEME: AppearanceTheme = "light"

/** Rutas con ambos temas terminados. El resto permanece en claro. */
export const DARK_THEME_ROUTE_PREFIXES = [
  "/analitica-v2/dashboard",
  "/analitica-v2/costos",
  "/analitica-v2/margenes",
  "/analitica-v2/packs",
  "/inteligencia-comercial/catalogo",
  // Graphite Premium 5.2 — Etapa B1: Pre-despacho OC renovado a tokens.
  "/distribuidora/orders",
] as const

export function isDarkThemeRoute(pathname: string | null | undefined): boolean {
  if (!pathname) return false
  return DARK_THEME_ROUTE_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  )
}

export function readAppearanceTheme(): AppearanceTheme {
  if (typeof window === "undefined") return DEFAULT_THEME
  return window.localStorage.getItem(APPEARANCE_STORAGE_KEY) === "dark" ? "dark" : DEFAULT_THEME
}

export function storeAppearanceTheme(theme: AppearanceTheme): void {
  if (typeof window === "undefined") return
  window.localStorage.setItem(APPEARANCE_STORAGE_KEY, theme)
}

/**
 * Script inline (anti-parpadeo): se ejecuta durante el parseo del contenedor
 * del dashboard, antes del primer pintado del contenido, y agrega la clase
 * `dark` al propio contenedor cuando corresponde. Después React sincroniza
 * el mismo estado desde la preferencia persistida, sin parpadeos.
 */
export const APPEARANCE_BOOTSTRAP_SCRIPT = `(function(){try{var t=window.localStorage.getItem("${APPEARANCE_STORAGE_KEY}");var ok=${JSON.stringify(
  DARK_THEME_ROUTE_PREFIXES,
)}.some(function(p){return window.location.pathname===p||window.location.pathname.indexOf(p+"/")===0});if(t==="dark"&&ok&&document.currentScript&&document.currentScript.parentElement){document.currentScript.parentElement.classList.add("dark")}}catch(e){}})();`
