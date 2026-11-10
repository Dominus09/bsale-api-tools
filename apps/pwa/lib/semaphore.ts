/** Semáforo operativo PWA 2.0 (alineado con API delivery_semaphore). */

export type SemaphoreColor = "green" | "yellow" | "red" | "neutral" | "active"

export type SemaphoreInfo = {
  color: SemaphoreColor
  code: string
  labelEs: string
  cssClass: string
}

const REASON_LABEL: Record<string, string> = {
  closed_store: "Local cerrado",
  not_found: "Local no encontrado",
  rejected_order: "Pedido rechazado",
  absent_customer: "Cliente ausente",
  other: "No entregado",
}

export function resolveSemaphore(opts: {
  status?: string | null
  resultReason?: string | null
  semaphoreColor?: string | null
  attentionLabel?: string | null
}): SemaphoreInfo {
  if (opts.semaphoreColor) {
    const color = opts.semaphoreColor as SemaphoreColor
    return {
      color,
      code: opts.status || color,
      labelEs: opts.attentionLabel || labelFor(opts.status, opts.resultReason),
      cssClass: `sem-${color}`,
    }
  }
  const st = (opts.status || "").trim()
  const reason = (opts.resultReason || "").trim() || null
  if (st === "completed") {
    return { color: "green", code: "completed", labelEs: "Entrega completa", cssClass: "sem-green" }
  }
  if (st === "partial") {
    return { color: "yellow", code: "partial", labelEs: "Entrega parcial", cssClass: "sem-yellow" }
  }
  if (st === "in_progress") {
    return { color: "active", code: "in_progress", labelEs: "En atención", cssClass: "sem-active" }
  }
  if (st === "not_delivered") {
    return {
      color: "red",
      code: reason || "not_delivered",
      labelEs: reason ? REASON_LABEL[reason] || "No entregado" : "No entregado",
      cssClass: "sem-red",
    }
  }
  return {
    color: "neutral",
    code: "pending",
    labelEs: opts.attentionLabel || "Pendiente",
    cssClass: "sem-neutral",
  }
}

function labelFor(status?: string | null, reason?: string | null) {
  if (status === "completed") return "Entrega completa"
  if (status === "partial") return "Entrega parcial"
  if (status === "in_progress") return "En atención"
  if (status === "not_delivered" && reason) return REASON_LABEL[reason] || "No entregado"
  if (status === "not_delivered") return "No entregado"
  return "Pendiente"
}

export function mapMarkerClass(color: SemaphoreColor): string {
  switch (color) {
    case "green":
      return "map-marker map-marker-done"
    case "yellow":
      return "map-marker map-marker-issue"
    case "red":
      return "map-marker map-marker-pending"
    case "active":
      return "map-marker map-marker-active"
    default:
      return "map-marker map-marker-neutral"
  }
}
