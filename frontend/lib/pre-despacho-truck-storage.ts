/** Asignaciones camión por OC en pre-despacho (persistencia de sesión). */

const STORAGE_KEY = "distribuidora_pre_despacho_truck_assignments_v1"

type StoredPayload = {
  dateFrom: string
  dateTo: string
  byDoc: Record<string, number>
}

function readRaw(): StoredPayload | null {
  if (typeof window === "undefined") return null
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw?.trim()) return null
    const data = JSON.parse(raw) as StoredPayload
    if (!data?.dateFrom || !data?.dateTo || typeof data.byDoc !== "object") return null
    return data
  } catch {
    return null
  }
}

export function readPreDespachoTruckAssignments(
  dateFrom: string,
  dateTo: string,
): Record<number, number | null> {
  const data = readRaw()
  if (!data || data.dateFrom !== dateFrom || data.dateTo !== dateTo) return {}
  const out: Record<number, number | null> = {}
  for (const [k, v] of Object.entries(data.byDoc)) {
    const docId = Number(k)
    if (Number.isFinite(docId) && Number.isFinite(v) && v > 0) {
      out[docId] = v
    }
  }
  return out
}

export function writePreDespachoTruckAssignments(
  dateFrom: string,
  dateTo: string,
  byDoc: Record<number, number | null>,
): void {
  if (typeof window === "undefined") return
  const compact: Record<string, number> = {}
  for (const [k, v] of Object.entries(byDoc)) {
    if (v != null && Number.isFinite(v) && v > 0) compact[k] = v
  }
  const payload: StoredPayload = { dateFrom, dateTo, byDoc: compact }
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(payload))
}

export function mergeTruckAssignmentsForRows(
  dateFrom: string,
  dateTo: string,
  documentIds: number[],
  current: Record<number, number | null>,
): Record<number, number | null> {
  const stored = readPreDespachoTruckAssignments(dateFrom, dateTo)
  const next: Record<number, number | null> = { ...stored, ...current }
  for (const id of documentIds) {
    if (current[id] !== undefined) next[id] = current[id]
    else if (stored[id] != null) next[id] = stored[id]
  }
  return next
}
