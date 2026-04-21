/**
 * Constantes y tipos base para planificación logística / ORS (Distribuidora).
 * El cálculo de rutas se integrará después; aquí solo estructura y depósito.
 */

export const BASE_COORDS = {
  lat: -43.13150730062163,
  lng: -73.6391717827585,
} as const

/** Stops por camión + depósito; listo para alimentar ORS más adelante. */
export type LogisticsTruckRouteStub = {
  truckId: number
  documentIds: number[]
  /** Coordenadas cliente por documento (mismo orden que ``documentIds``). */
  clientCoords: { document_id: number; lat: number; lng: number }[]
  depotLat: number
  depotLng: number
}

export function normMunicipality(m: string | null | undefined): string {
  const t = (m ?? "").trim()
  return t.length ? t : "Sin comuna"
}

function rowHasGeo(row: {
  has_georef?: boolean | null
  lat?: number | null
  lng?: number | null
}): boolean {
  return Boolean(row.has_georef && row.lat != null && row.lng != null)
}

/**
 * Cluster simple: ``{municipality} Norte|Centro|Sur`` por percentil de latitud
 * dentro de la misma comuna (Chile: lat mayor = más al norte).
 */
export function buildClusterLabelByDocumentId(
  rows: {
    document_id: number
    municipality?: string | null
    lat?: number | null
    lng?: number | null
    has_georef?: boolean | null
  }[],
): Map<number, string> {
  const byMuni = new Map<string, number[]>()
  for (const r of rows) {
    if (!rowHasGeo(r) || r.lat == null) continue
    const m = normMunicipality(r.municipality)
    const lat = Number(r.lat)
    if (!Number.isFinite(lat)) continue
    if (!byMuni.has(m)) byMuni.set(m, [])
    byMuni.get(m)!.push(lat)
  }

  const out = new Map<number, string>()
  for (const r of rows) {
    const m = normMunicipality(r.municipality)
    if (!rowHasGeo(r) || r.lat == null) {
      out.set(r.document_id, m === "Sin comuna" ? "—" : m)
      continue
    }
    const lat = Number(r.lat)
    const lats = byMuni.get(m)
    if (!lats || lats.length < 2) {
      out.set(r.document_id, m)
      continue
    }
    const northier = lats.filter((x) => x > lat).length
    const frac = northier / lats.length
    let band: string
    if (frac < 1 / 3) band = "Norte"
    else if (frac < 2 / 3) band = "Centro"
    else band = "Sur"
    out.set(r.document_id, `${m} ${band}`)
  }
  return out
}

export function buildRouteStubsFromAssignments(params: {
  truckIdByDoc: Record<number, number | null>
  rows: {
    document_id: number
    lat?: number | null
    lng?: number | null
    has_georef?: boolean | null
  }[]
  validTruckIds: Set<number>
}): LogisticsTruckRouteStub[] {
  const byTruck = new Map<number, number[]>()
  for (const r of params.rows) {
    const tid = params.truckIdByDoc[r.document_id]
    if (tid == null || !params.validTruckIds.has(tid)) continue
    if (!rowHasGeo(r) || r.lat == null || r.lng == null) continue
    if (!byTruck.has(tid)) byTruck.set(tid, [])
    byTruck.get(tid)!.push(r.document_id)
  }
  const stubs: LogisticsTruckRouteStub[] = []
  for (const [truckId, documentIds] of byTruck) {
    const clientCoords = documentIds.map((id) => {
      const row = params.rows.find((x) => x.document_id === id)!
      return {
        document_id: id,
        lat: Number(row.lat),
        lng: Number(row.lng),
      }
    })
    stubs.push({
      truckId,
      documentIds,
      clientCoords,
      depotLat: BASE_COORDS.lat,
      depotLng: BASE_COORDS.lng,
    })
  }
  return stubs
}
