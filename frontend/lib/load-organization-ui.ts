/** Helpers de presentación para organización anticipada de carga (sin auto-asignar). */

export type LoadOrgSortKey =
  | "venta_desc"
  | "sku_desc"
  | "cajas_desc"
  | "peso_desc"
  | "nombre_asc"

export type LoadOrgClientLike = {
  client_id?: number | null
  client_name: string
  venta_oc_clp: number
  sku_distinct: number
  cajas_equivalentes?: number | null
  peso_conocido_kg: number
}

export function clientSelectionKey(c: {
  client_id?: number | null
  client_name: string
}): string {
  if (c.client_id != null) return `id:${c.client_id}`
  return `name:${c.client_name}`
}

export function encodeBatchRoleDescription(
  role: "priority" | "consolidated" | null | undefined,
  note?: string | null,
): string | undefined {
  const clean = stripBatchRoleDescription(note)
  if (role === "priority") {
    return clean ? `role=priority\n${clean}` : "role=priority"
  }
  if (role === "consolidated") {
    return clean ? `role=consolidated\n${clean}` : "role=consolidated"
  }
  return clean || undefined
}

export function stripBatchRoleDescription(description?: string | null): string {
  const text = (description || "").trim()
  if (!text) return ""
  const lines = text.split(/\r?\n/)
  const first = lines[0]?.trim().toLowerCase() || ""
  if (first.startsWith("role=priority") || first.startsWith("role=consolidated")) {
    return lines.slice(1).join("\n").trim()
  }
  return text
}

export function sortLoadOrgClients<T extends LoadOrgClientLike>(
  clients: T[],
  sortKey: LoadOrgSortKey,
): T[] {
  const copy = [...clients]
  copy.sort((a, b) => {
    switch (sortKey) {
      case "sku_desc":
        return b.sku_distinct - a.sku_distinct || b.venta_oc_clp - a.venta_oc_clp
      case "cajas_desc": {
        const ca = a.cajas_equivalentes ?? -1
        const cb = b.cajas_equivalentes ?? -1
        return cb - ca || b.venta_oc_clp - a.venta_oc_clp
      }
      case "peso_desc":
        return b.peso_conocido_kg - a.peso_conocido_kg || b.venta_oc_clp - a.venta_oc_clp
      case "nombre_asc":
        return a.client_name.localeCompare(b.client_name, "es")
      case "venta_desc":
      default:
        return b.venta_oc_clp - a.venta_oc_clp || a.client_name.localeCompare(b.client_name, "es")
    }
  })
  return copy
}

export function priorityReviewNeeded(
  role: "priority" | "consolidated" | null | undefined,
  clientCount: number,
): boolean {
  return role === "priority" && clientCount > 2
}
