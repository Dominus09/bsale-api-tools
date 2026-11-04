/** Helpers puros de selección/asignación masiva (sin llamadas al backend). */

export type FolioKey = string

export type TruckSelectOption = {
  id: number
  name?: string | null
  plate?: string | null
}

/**
 * Etiqueta de selector: nombre · patente · #id cuando hay homónimos.
 * El valor del Select sigue siendo siempre ``truck_id``.
 */
export function formatTruckSelectLabel(
  truck: TruckSelectOption,
  allTrucks: TruckSelectOption[] = [],
): string {
  const name = (truck.name || `Camión ${truck.id}`).trim()
  const plate = (truck.plate || "").trim()
  const nameKey = name.toLowerCase()
  const sameNameCount = allTrucks.filter(
    (t) => (t.name || "").trim().toLowerCase() === nameKey,
  ).length
  const parts = [name]
  if (plate) parts.push(plate)
  if (sameNameCount > 1 || !plate) parts.push(`#${truck.id}`)
  return parts.join(" · ")
}

/** Camiones con al menos una OC asignada (orden de primera aparición). */
export function truckIdsFromAssignments(
  assignmentByFolio: Record<string, number>,
): number[] {
  const seen = new Set<number>()
  const out: number[] = []
  for (const raw of Object.values(assignmentByFolio)) {
    const tid = Number(raw)
    if (!Number.isFinite(tid) || tid <= 0 || seen.has(tid)) continue
    seen.add(tid)
    out.push(tid)
  }
  return out
}

export function folioKey(folio: number | string): FolioKey {
  return String(folio)
}

export function toggleFolioInSelection(
  selected: Set<FolioKey>,
  folio: number | string,
  checked: boolean,
): Set<FolioKey> {
  const next = new Set(selected)
  const key = folioKey(folio)
  if (checked) next.add(key)
  else next.delete(key)
  return next
}

export function selectAllFolios(folios: Array<number | string>): Set<FolioKey> {
  return new Set(folios.map(folioKey))
}

export function clearFolioSelection(): Set<FolioKey> {
  return new Set()
}

/** Reemplaza la asignación de cada folio (un camión por OC). truckId null = sin asignar. */
export function applyBulkTruckAssignment(
  assignmentByFolio: Record<string, number>,
  selectedFolios: Iterable<number | string>,
  truckId: number | null,
): Record<string, number> {
  const next = { ...assignmentByFolio }
  for (const folio of selectedFolios) {
    const key = folioKey(folio)
    if (truckId == null) delete next[key]
    else next[key] = truckId
  }
  return next
}

export function uniqueMunicipalities(
  items: Array<{ client?: { municipality?: string | null } | null }>,
): string[] {
  const set = new Set<string>()
  for (const item of items) {
    const m = (item.client?.municipality || "").trim()
    if (m) set.add(m)
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
}

export function filterByMunicipality<
  T extends { client?: { municipality?: string | null } | null },
>(items: T[], municipality: string | null | undefined): T[] {
  if (!municipality || municipality === "__all__") return items
  return items.filter(
    (i) => (i.client?.municipality || "").trim() === municipality,
  )
}

export function visibleAssignableFolios(
  items: Array<{
    folio?: number | null
    blocking?: boolean
    status?: string
  }>,
): number[] {
  const out: number[] = []
  const seen = new Set<number>()
  for (const item of items) {
    const folio = Number(item.folio)
    if (!Number.isFinite(folio) || seen.has(folio)) continue
    if (item.blocking) continue
    if (
      item.status === "not_found" ||
      item.status === "invalid_folio" ||
      item.status === "duplicate_in_list" ||
      item.status === "cancelled_or_replaced"
    ) {
      continue
    }
    seen.add(folio)
    out.push(folio)
  }
  return out
}
