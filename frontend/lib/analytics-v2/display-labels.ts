/** Etiquetas comerciales visibles (sin IDs técnicos de Bsale). */

const OFFICE_TECH_SUFFIX = /\s*[·/\-]\s*Oficina\s+\d+\s*$/i

/** Denominación administrativa oficial empresa 3 (La Quillotana SpA). */
const OFFICE_ADMIN_LABELS: Record<number, Record<number, string>> = {
  3: {
    1: "Bodega Central",
    2: "Bodega 1",
    3: "Supermercado",
    4: "Quillotana I",
    5: "Quillotana II",
    6: "Bajas",
  },
}

export function formatPriceListLabel(
  list: { id?: number | null; name?: string | null; label?: string | null } | null | undefined,
): string {
  const text = (list?.label || list?.name || "").trim()
  if (text && !/^Lista(\s+de\s+precios)?\s+\d+$/i.test(text)) return text
  if (list?.id != null) return `Lista sin nombre — requiere corrección del maestro (ref. ${list.id})`
  return "Lista sin nombre — requiere corrección del maestro"
}

function normalizeQuillotanaOffice(cleaned: string): string {
  if (/^quillotana\s*1$/i.test(cleaned)) return "Quillotana I"
  if (/^quillotana\s*2$/i.test(cleaned)) return "Quillotana II"
  return cleaned
}

export function formatOfficeLabel(
  name: string | null | undefined,
  officeId?: number | null,
  companyId?: number | null,
): string {
  if (companyId != null && officeId != null) {
    const alias = OFFICE_ADMIN_LABELS[companyId]?.[officeId]
    if (alias) return alias
  }
  const cleaned = normalizeQuillotanaOffice(
    (name ?? "").trim().replace(OFFICE_TECH_SUFFIX, "").trim(),
  )
  if (cleaned) return cleaned
  if (officeId != null) return `Sucursal sin nombre — requiere corrección del maestro (ref. ${officeId})`
  return "Sucursal sin nombre — requiere corrección del maestro"
}
