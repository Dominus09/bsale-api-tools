const STATUS_LABELS: Record<string, string> = {
  planned: "Planificado",
  invoicing: "En facturación",
  ready_for_picking: "Listo para picking",
  picking_generated: "Picking generado",
  dispatched: "Despachado",
  delivered: "Entregado",
  closed: "Cerrado",
  draft: "Borrador",
}

export function statusLabel(status: string | null | undefined): string {
  if (!status) return "—"
  return STATUS_LABELS[status] || status
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—"
  const d = String(value).slice(0, 10)
  const [y, m, day] = d.split("-")
  if (!y || !m || !day) return d
  return `${day}/${m}/${y}`
}
