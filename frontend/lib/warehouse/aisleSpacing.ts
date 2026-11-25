import type { WarehouseElement, ElementGeometry } from "./types"

const EPS = 0.0001
const round = (n: number) => Math.round(n * 10000) / 10000
const active = (e: WarehouseElement) => !e.pendingRetire && !e.pendingRemoveGeometry

type Row = { elements: WarehouseElement[]; min: number; max: number }
export type SpacingAisle = { element: WarehouseElement; low: Row; high: Row }
export type SpacingGap = { key: string; left: string; right: string; currentM: number }
export type SpacingInspection = { aisles: SpacingAisle[]; gaps: SpacingGap[] }
export type SpacingPlan = {
  elements: WarehouseElement[]
  changedKeys: string[]
  gaps: Array<SpacingGap & { proposedM: number }>
  interior: Array<{ code: string; currentM: number; proposedM: number }>
  errors: string[]
  circulationWarnings: string[]
}
type Bounds = { x0: number; x1: number; y0: number; y1: number }

function aligned(g: ElementGeometry) {
  return [g.x_m, g.y_m, g.width_m, g.depth_m, g.rotation_deg].every(Number.isFinite) &&
    g.width_m > 0 && g.depth_m > 0 && Math.abs(g.rotation_deg % 360) < EPS &&
    g.geom_type === "rect"
}

function bounds(g: ElementGeometry): Bounds {
  const r = g.rotation_deg * Math.PI / 180
  const w = Math.abs(Math.cos(r)) * g.width_m + Math.abs(Math.sin(r)) * g.depth_m
  const d = Math.abs(Math.sin(r)) * g.width_m + Math.abs(Math.cos(r)) * g.depth_m
  const cx = g.x_m + g.width_m / 2, cy = g.y_m + g.depth_m / 2
  return { x0: cx - w / 2, x1: cx + w / 2, y0: cy - d / 2, y1: cy + d / 2 }
}

function intersection(a: Bounds, b: Bounds) {
  const w = Math.min(a.x1, b.x1) - Math.max(a.x0, b.x0)
  const d = Math.min(a.y1, b.y1) - Math.max(a.y0, b.y0)
  return w > EPS && d > EPS ? w * d : 0
}

/** Identifica filas por su geometría, no por el orden de códigos ni por índices
 * del generador. Sólo admite pasillos rectangulares alineados, con dos filas
 * uniformes. No inventa filas si se modificaron posiciones individualmente.
 */
export function inspectAisleSpacing(elements: WarehouseElement[], sectorId: number): SpacingInspection {
  const all = elements.filter(active)
  const sector = all.find(e => e.physical.id === sectorId && e.physical.element_type === "sector")
  if (!sector || !aligned(sector.geometry)) throw new Error("El sector no tiene una geometría rectangular alineada.")
  const aisles = all.filter(e => e.physical.parent_id === sectorId && e.physical.element_type === "aisle")
    .sort((a, b) => a.geometry.x_m - b.geometry.x_m)
    .map(element => {
      const g = element.geometry
      const code = element.physical.code
      if (element.physical.id <= 0 || !aligned(g)) throw new Error(code + ": guarde el pasillo rectangular y alineado antes de redistribuir.")
      if (g.x_m < sector.geometry.x_m - EPS || g.x_m + g.width_m > sector.geometry.x_m + sector.geometry.width_m + EPS) {
        throw new Error(code + ": fuera del largo nominal del sector.")
      }
      const positions = all.filter(e => e.physical.parent_id === element.physical.id && e.physical.element_type === "position")
        .sort((a, b) => a.geometry.x_m - b.geometry.x_m)
      const rows: Row[] = []
      for (const p of positions) {
        const pg = p.geometry
        if (p.physical.id <= 0 || !aligned(pg)) throw new Error(p.physical.code + ": posición sin guardar, girada o no rectangular.")
        const min = pg.x_m, max = min + pg.width_m
        if (min < g.x_m - EPS || max > g.x_m + g.width_m + EPS) throw new Error(p.physical.code + ": sale del ancho X de su pasillo.")
        const row = rows.find(r => Math.abs(r.min - min) <= EPS && Math.abs(r.max - max) <= EPS)
        if (row) row.elements.push(p)
        else rows.push({ elements: [p], min, max })
      }
      if (rows.length !== 2 || rows[0].max > rows[1].min + EPS) {
        throw new Error(code + ": se necesitan dos filas alineadas sin superposición. Revise posiciones movidas individualmente.")
      }
      return { element, low: rows[0], high: rows[1] }
    })
  if (aisles.length < 2) throw new Error("Se necesitan al menos dos pasillos poblados en el sector.")
  const gaps: SpacingGap[] = []
  for (let i = 0; i + 1 < aisles.length; i++) {
    const a = aisles[i], b = aisles[i + 1]
    const ag = a.element.geometry, bg = b.element.geometry
    if (ag.x_m + ag.width_m > bg.x_m + EPS) throw new Error(a.element.physical.code + " y " + b.element.physical.code + ": límites nominales superpuestos.")
    // Este modo se refiere a filas paralelas enfrentadas a lo largo de X.
    if (Math.min(ag.y_m + ag.depth_m, bg.y_m + bg.depth_m) <= Math.max(ag.y_m, bg.y_m) + EPS) {
      throw new Error("Los pasillos no tienen profundidad Y en común.")
    }
    gaps.push({ key: a.element.clientKey + "|" + b.element.clientKey,
      left: a.element.physical.code, right: b.element.physical.code,
      currentM: round(b.low.min - a.high.max) })
  }
  return { aisles, gaps }
}

/** Ajusta sólo X de las filas exteriores de cada par. Las fronteras de
 * pasillo, las estructuras, Y, dimensiones, códigos e IDs quedan intactos.
 * B es distancia libre entre huellas de pallets, no entre marcos nominales.
 */
export function planAisleSpacing(
  elements: WarehouseElement[],
  sectorId: number,
  targetsM: Record<string, number>,
  floorLengthM: number,
  floorWidthM: number,
): SpacingPlan {
  const { aisles, gaps } = inspectAisleSpacing(elements, sectorId)
  const errors: string[] = []
  const deltas = new Map<string, number>()
  const addDelta = (row: Row, delta: number) => {
    for (const p of row.elements) deltas.set(p.clientKey, round(delta))
  }
  for (let i = 0; i < gaps.length; i++) {
    const gap = gaps[i], a = aisles[i], b = aisles[i + 1]
    const target = targetsM[gap.key] ?? gap.currentM
    if (!Number.isFinite(target) || target < 0) {
      errors.push(gap.left + " ↔ " + gap.right + ": separación inválida.")
      continue
    }
    if (Math.abs(target - gap.currentM) <= EPS) continue
    const ag = a.element.geometry, bg = b.element.geometry
    const leftWidth = a.high.max - a.high.min, rightWidth = b.low.max - b.low.min
    // Buscar el punto medio más cercano al existente que permita el objetivo
    // sin salir de los límites X de ninguno de los pasillos.
    const minEdge = Math.max(ag.x_m + leftWidth, bg.x_m - target)
    const maxEdge = Math.min(ag.x_m + ag.width_m, bg.x_m + bg.width_m - rightWidth - target)
    if (minEdge > maxEdge + EPS) {
      errors.push(gap.left + " ↔ " + gap.right + ": la separación no cabe dentro de los límites nominales.")
      continue
    }
    const idealEdge = (a.high.max + b.low.min - target) / 2
    const leftEdge = round(Math.min(maxEdge, Math.max(minEdge, idealEdge)))
    addDelta(a.high, leftEdge - a.high.max)
    addDelta(b.low, leftEdge + target - b.low.min)
  }

  const rowDelta = (row: Row) => deltas.get(row.elements[0].clientKey) ?? 0
  const interior = aisles.map(a => {
    const currentM = round(a.high.min - a.low.max)
    const proposedM = round(a.high.min + rowDelta(a.high) - a.low.max - rowDelta(a.low))
    if (proposedM < -EPS) errors.push(a.element.physical.code + ": las dos filas se superponen; reduzca B.")
    return { code: a.element.physical.code, currentM, proposedM }
  })
  const proposedGaps = gaps.map((g, i) => ({ ...g,
    proposedM: round(aisles[i + 1].low.min + rowDelta(aisles[i + 1].low) - aisles[i].high.max - rowDelta(aisles[i].high)) }))
  const changedKeys = [...deltas.entries()].filter(([, d]) => Math.abs(d) > EPS / 2).map(([key]) => key)
  const changed = new Set(changedKeys)
  const proposed = elements.map(e => {
    if (!changed.has(e.clientKey)) return e
    const meta = { ...e.physical.meta_json, ...e.geometry.meta_json, ...e.geometry.constraints_json }
    if (e.geometry.validation_status === "validated" || meta.stacking_authorized === true || meta.safe_storage_authorized === true) {
      errors.push(e.physical.code + ": posición validada/autorizada; requiere revisión individual antes de moverla.")
    }
    return { ...e, dirty: true, geometry: { ...e.geometry,
      x_m: round(e.geometry.x_m + (deltas.get(e.clientKey) ?? 0)), validation_status: "pending_validation" as const } }
  })

  const existing = new Map(elements.map(e => [e.clientKey, e]))
  const all = proposed.filter(active)
  const byId = new Map(all.map(e => [e.physical.id, e]))
  const warnings = new Set<string>()
  const seen = new Set<string>()
  for (const p of all.filter(e => changed.has(e.clientKey))) {
    const g = p.geometry
    const parent = byId.get(p.physical.parent_id!)
    if (!parent || g.x_m < parent.geometry.x_m - EPS || g.x_m + g.width_m > parent.geometry.x_m + parent.geometry.width_m + EPS) {
      errors.push(p.physical.code + ": sale del límite X de su pasillo.")
    }
    const pb = bounds(g)
    if (pb.x0 < -EPS || pb.y0 < -EPS || pb.x1 > floorLengthM + EPS || pb.y1 > floorWidthM + EPS) {
      errors.push(p.physical.code + ": fuera del perímetro de bodega.")
    }
    const before = bounds(existing.get(p.clientKey)!.geometry)
    for (const other of all) {
      if (other.clientKey === p.clientKey) continue
      const kind = other.physical.element_type
      if (kind === "facility_boundary" || kind === "sector" || kind === "aisle") continue
      const ob = bounds(other.geometry)
      const area = intersection(pb, ob)
      if (!area) continue
      const pair = [p.clientKey, other.clientKey].sort().join("|")
      if (seen.has(pair)) continue
      seen.add(pair)
      if (kind === "circulation") {
        const previous = intersection(before, ob)
        if (area > previous + EPS) errors.push(p.physical.code + ": nueva o mayor invasión de " + other.physical.code + ".")
        else warnings.add(p.physical.code + ": conserva extensión observada hacia " + other.physical.code + ".")
      } else {
        // AABB conservador para obstáculos girados. No autoriza superponer
        // posiciones, estructuras, zonas sueltas ni espacios restringidos.
        errors.push(p.physical.code + " se superpone con " + other.physical.code + ".")
      }
    }
  }
  return { elements: proposed, changedKeys, gaps: proposedGaps, interior,
    errors: [...new Set(errors)], circulationWarnings: [...warnings] }
}

/** Comparar validación completa antes/después: los conflictos preexistentes
 * siguen visibles en el editor y no se presentan como resueltos.
 */
export function newSpacingBlockers(before: Record<string, unknown>[], after: Record<string, unknown>[]) {
  const key = (i: Record<string, unknown>) => JSON.stringify([
    i.severity, i.code, String(i.element_ref ?? i.element_code ?? ""), i.other_code ?? "",
  ])
  const old = new Set(before.map(key))
  return after.filter(i => !old.has(key(i)) && (
    i.severity === "integrity_error" || i.severity === "geom_conflict" ||
    i.code === "circulation_invasion" || i.code === "outside_parent_sector"
  ))
}
