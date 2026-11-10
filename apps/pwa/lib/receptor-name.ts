/**
 * Resuelve el nombre del receptor priorizando el valor del DOM.
 * En iOS Safari, Autofill/QuickType puede pintar el input sin actualizar
 * el state de React controlado; el submit debe leer el DOM.
 */
export function resolveReceptorName(domValue: string | null | undefined, stateValue: string): string {
  const dom = String(domValue ?? "").trim()
  const state = String(stateValue ?? "").trim()
  return dom || state
}

export function receptorNameIsValid(name: string, refused: boolean): boolean {
  if (refused) return true
  return name.trim().length >= 2
}
