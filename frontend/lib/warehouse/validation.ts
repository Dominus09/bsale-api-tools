import type { WarehouseElement, WarehouseElementType } from "@/lib/warehouse/types"

export type ValidationIssue = {
  severity: "error" | "warning"
  code: string
  message: string
  clientKey?: string
}

const HIERARCHY_OK_OVERLAP: Set<WarehouseElementType> = new Set([
  "facility_boundary",
  "sector",
  "aisle",
  "circulation",
])

function aabb(
  e: WarehouseElement,
): { minX: number; minY: number; maxX: number; maxY: number } {
  const g = e.geometry
  // Aproximación AABB sin rotación completa (suficiente para advertencias básicas)
  const rad = ((g.rotation_deg || 0) * Math.PI) / 180
  const cos = Math.abs(Math.cos(rad))
  const sin = Math.abs(Math.sin(rad))
  const w = g.width_m * cos + g.depth_m * sin
  const d = g.width_m * sin + g.depth_m * cos
  const cx = g.x_m + g.width_m / 2
  const cy = g.y_m + g.depth_m / 2
  return {
    minX: cx - w / 2,
    minY: cy - d / 2,
    maxX: cx + w / 2,
    maxY: cy + d / 2,
  }
}

function overlaps(
  a: { minX: number; minY: number; maxX: number; maxY: number },
  b: { minX: number; minY: number; maxX: number; maxY: number },
): boolean {
  // Igual tolerancia numérica que SAT del backend (metros); un borde compartido no solapa.
  const eps = 1e-9
  return !(a.maxX <= b.minX + eps || b.maxX <= a.minX + eps || a.maxY <= b.minY + eps || b.maxY <= a.minY + eps)
}

function isAncestor(child: WarehouseElement, ancestorId: number, all: WarehouseElement[]): boolean {
  let cur: WarehouseElement | undefined = child
  const seen = new Set<string>()
  while (cur?.physical.parent_id != null) {
    if (cur.physical.parent_id === ancestorId) return true
    if (seen.has(cur.clientKey)) break
    seen.add(cur.clientKey)
    cur = all.find((x) => x.physical.id === cur!.physical.parent_id)
  }
  return false
}

export function validateElements(
  elements: WarehouseElement[],
  floor: { length_m: number | null; width_m: number | null },
): ValidationIssue[] {
  const issues: ValidationIssue[] = []
  const active = elements.filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry)
  const codes = new Map<string, string>()

  for (const e of active) {
    const g = e.geometry
    if (!(g.width_m > 0) || !(g.depth_m > 0)) {
      issues.push({
        severity: "error",
        code: "dims_positive",
        message: `${e.physical.code}: ancho y profundidad deben ser > 0`,
        clientKey: e.clientKey,
      })
    }
    if (!e.physical.code?.trim() || !e.physical.name?.trim()) {
      issues.push({
        severity: "error",
        code: "identity_required",
        message: "Código y nombre son obligatorios",
        clientKey: e.clientKey,
      })
    }
    const key = e.physical.code.trim().toLowerCase()
    if (key) {
      if (codes.has(key)) {
        issues.push({
          severity: "error",
          code: "duplicate_code",
          message: `Código duplicado en el piso: ${e.physical.code}`,
          clientKey: e.clientKey,
        })
      } else codes.set(key, e.clientKey)
    }
    if (e.physical.parent_id != null) {
      const parent = active.find((p) => p.physical.id === e.physical.parent_id)
      if (!parent && e.physical.parent_id > 0) {
        // parent puede existir sin geometría en este draft; no bloquear fuerte
      }
    }

    if (floor.length_m != null && floor.width_m != null) {
      const box = aabb(e)
      if (
        box.minX < -0.05 ||
        box.minY < -0.05 ||
        box.maxX > floor.length_m + 0.05 ||
        box.maxY > floor.width_m + 0.05
      ) {
        issues.push({
          severity: "warning",
          code: "outside_perimeter",
          message: `${e.physical.code}: parece fuera del perímetro del piso`,
          clientKey: e.clientKey,
        })
      }
    }
  }

  // Superposiciones: advertir solo entre pares no jerárquicos / no contenedor-contenido
  for (let i = 0; i < active.length; i++) {
    for (let j = i + 1; j < active.length; j++) {
      const a = active[i]
      const b = active[j]
      if (HIERARCHY_OK_OVERLAP.has(a.physical.element_type) && a.physical.id === b.physical.parent_id)
        continue
      if (HIERARCHY_OK_OVERLAP.has(b.physical.element_type) && b.physical.id === a.physical.parent_id)
        continue
      if (isAncestor(a, b.physical.id, active) || isAncestor(b, a.physical.id, active)) continue
      // Posición dentro de sector/pasillo: OK
      if (
        (a.physical.element_type === "position" &&
          (b.physical.element_type === "sector" || b.physical.element_type === "aisle")) ||
        (b.physical.element_type === "position" &&
          (a.physical.element_type === "sector" || a.physical.element_type === "aisle"))
      ) {
        continue
      }
      // Límite contiene todo
      if (
        a.physical.element_type === "facility_boundary" ||
        b.physical.element_type === "facility_boundary"
      ) {
        continue
      }
      if (!overlaps(aabb(a), aabb(b))) continue
      // Dos posiciones o estructuras que se cruzan → advertencia
      const bothLeaf =
        (a.physical.element_type === "position" || a.physical.element_type === "obstacle") &&
        (b.physical.element_type === "position" || b.physical.element_type === "obstacle")
      if (bothLeaf || a.physical.element_type === b.physical.element_type) {
        issues.push({
          severity: "warning",
          code: "overlap",
          message: `Posible solapamiento físico: ${a.physical.code} ↔ ${b.physical.code} (no es certificación de seguridad)`,
          clientKey: a.clientKey,
        })
      }
    }
  }

  return issues
}
