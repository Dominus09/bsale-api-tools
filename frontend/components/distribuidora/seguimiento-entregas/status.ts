/** Semáforos y etiquetas visuales — Seguimiento de entregas (solo UI). */

export type ResultTone = "completed" | "partial" | "not_delivered" | "pending" | "review"

export function resultTone(
  status?: string | null,
  requiresReview?: boolean,
): ResultTone {
  if (requiresReview) return "review"
  const s = (status || "").trim()
  if (s === "completed") return "completed"
  if (s === "partial") return "partial"
  if (s === "not_delivered") return "not_delivered"
  return "pending"
}

export const RESULT_LABEL: Record<string, string> = {
  completed: "Entrega completa",
  partial: "Entrega parcial",
  not_delivered: "No entregado",
  pending: "Pendiente",
  in_progress: "En atención",
}

export const TONE_DOT: Record<ResultTone, string> = {
  completed: "bg-emerald-500",
  partial: "bg-amber-400",
  not_delivered: "bg-[var(--lq-brand)]",
  pending: "bg-zinc-500",
  review: "bg-sky-400",
}

export const TONE_BADGE: Record<ResultTone, string> = {
  completed:
    "border-emerald-500/35 bg-emerald-500/15 text-emerald-300",
  partial: "border-amber-400/35 bg-amber-400/15 text-amber-200",
  not_delivered: "border-[var(--lq-brand)]/40 bg-[var(--lq-brand)]/20 text-red-200",
  pending: "border-white/15 bg-white/5 text-lq-on-graphite-muted",
  review: "border-sky-400/40 bg-sky-400/15 text-sky-200",
}

export const TONE_PROGRESS: Record<ResultTone, string> = {
  completed: "bg-emerald-500",
  partial: "bg-amber-400",
  not_delivered: "bg-[var(--lq-brand)]",
  pending: "bg-zinc-500",
  review: "bg-sky-400",
}

export function managementLabel(row: {
  delivered?: number
  partial?: number
  not_delivered?: number
  pending?: number
  clients_total?: number
}): string {
  const total = row.clients_total ?? 0
  const managed =
    (row.delivered ?? 0) + (row.partial ?? 0) + (row.not_delivered ?? 0)
  if (total <= 0) return "Sin clientes"
  if (managed <= 0) return "Sin gestiones"
  if ((row.pending ?? 0) <= 0) return "Ruta completada"
  return "En ruta"
}
