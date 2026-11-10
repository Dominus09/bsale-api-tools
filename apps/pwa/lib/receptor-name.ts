/**
 * Resuelve el nombre del receptor priorizando el valor real del DOM.
 * En iPhone Safari, Autofill/QuickType puede pintar el input sin actualizar
 * el state de React controlado.
 *
 * Prioridad:
 * 1. ref/DOM value
 * 2. FormData del formulario (si existe)
 * 3. state de React
 */
export function resolveReceptorName(
  domValue: string | null | undefined,
  stateValue: string,
  formDataValue?: string | null,
): string {
  const dom = String(domValue ?? "").trim()
  if (dom) return dom
  const fromForm = String(formDataValue ?? "").trim()
  if (fromForm) return fromForm
  return String(stateValue ?? "").trim()
}

export function resolveReceptorNameFromForm(
  input: HTMLInputElement | null | undefined,
  stateValue: string,
  form?: HTMLFormElement | null,
): string {
  const dom = input?.value
  let formVal: string | null = null
  if (form) {
    try {
      const fd = new FormData(form)
      const raw = fd.get(input?.name || "receptor_name")
      formVal = raw == null ? null : String(raw)
    } catch {
      formVal = null
    }
  }
  return resolveReceptorName(dom, stateValue, formVal)
}

export function receptorNameIsValid(name: string, refused: boolean): boolean {
  if (refused) return true
  return name.trim().length >= 2
}
