/** Formato de presentación para módulo Costos (sin lógica de negocio). */

export function formatMoneyCLP(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "Sin información"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Number(value))
}

export function formatPct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(Number(value))) return "Sin información"
  const n = Number(value)
  const sign = n > 0 ? "+" : ""
  return `${sign}${n.toFixed(digits)}%`
}

export function formatDateShort(value: string | null | undefined): string {
  if (!value) return "Sin información"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return "Sin información"
  return d.toLocaleDateString("es-CL", { dateStyle: "short" })
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "Sin información"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return "Sin información"
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

export function daysBetween(from: Date, to: Date): number {
  const ms = to.getTime() - from.getTime()
  return Math.floor(ms / (1000 * 60 * 60 * 24))
}
