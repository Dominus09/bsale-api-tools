/**
 * Día de entrega detectado desde observaciones OC (pre-despacho).
 * Espejo de backend/utils/delivery_day_detect.py
 */

const DAY_TOKENS = [
  "lunes",
  "martes",
  "miercoles",
  "jueves",
  "viernes",
  "sabado",
  "domingo",
] as const

export type DeliveryDayToken = (typeof DAY_TOKENS)[number]

const DAY_LABEL: Record<DeliveryDayToken, string> = {
  lunes: "Lunes",
  martes: "Martes",
  miercoles: "Miércoles",
  jueves: "Jueves",
  viernes: "Viernes",
  sabado: "Sábado",
  domingo: "Domingo",
}

const DAY_RE = new RegExp(`\\b(${DAY_TOKENS.join("|")})\\b`, "gi")
const DELIVERY_CTX_RE = /\b(entrega|retiro|reparto|despacho)\b/i

function stripAccents(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
}

function normalizeDayToken(raw: string | null | undefined): DeliveryDayToken | null {
  if (!raw?.trim()) return null
  const s = stripAccents(raw).replace(/[^a-z]/g, "")
  return (DAY_TOKENS as readonly string[]).includes(s) ? (s as DeliveryDayToken) : null
}

export function detectDeliveryDayFromObservation(
  text: string | null | undefined,
): DeliveryDayToken | null {
  if (!text?.trim()) return null
  const norm = stripAccents(text)
  const matches = [...norm.matchAll(DAY_RE)]
  if (matches.length === 0) return null
  if (matches.length === 1) return normalizeDayToken(matches[0][1])

  let best: DeliveryDayToken | null = null
  let bestScore = -1
  for (const m of matches) {
    const token = normalizeDayToken(m[1])
    if (!token) continue
    const idx = m.index ?? 0
    const window = norm.slice(Math.max(0, idx - 48), idx + m[0].length + 48)
    let score = idx / Math.max(norm.length, 1)
    if (DELIVERY_CTX_RE.test(window)) score += 10
    if (score >= bestScore) {
      bestScore = score
      best = token
    }
  }
  return best
}

export function formatDeliveryDayLabel(
  token: string | null | undefined,
): string {
  if (!token?.trim()) return "Sin día"
  const k = normalizeDayToken(token)
  if (!k) return "Sin día"
  return DAY_LABEL[k]
}

const DAY_BADGE_CLASS: Record<DeliveryDayToken, string> = {
  lunes: "border-slate-300 bg-slate-50 text-slate-800 dark:bg-slate-900/40",
  martes: "border-slate-300 bg-slate-50 text-slate-800 dark:bg-slate-900/40",
  miercoles: "border-blue-200 bg-blue-50 text-blue-900 dark:bg-blue-950/40",
  jueves: "border-indigo-200 bg-indigo-50 text-indigo-900 dark:bg-indigo-950/40",
  viernes: "border-violet-200 bg-violet-50 text-violet-900 dark:bg-violet-950/40",
  sabado: "border-amber-300 bg-amber-50 text-amber-950 dark:bg-amber-950/40",
  domingo: "border-rose-200 bg-rose-50 text-rose-900 dark:bg-rose-950/40",
}

export function deliveryDayBadgeClass(token: string | null | undefined): string {
  const k = normalizeDayToken(token)
  if (!k) return "border-border bg-muted/40 text-muted-foreground"
  return DAY_BADGE_CLASS[k]
}

/** Etiqueta visual para tabla pre-despacho (incluye Retiro si aplica). */
export function formatPreDespachoDeliveryDay(row: {
  observaciones?: string | null
  dia_entrega_detectado?: string | null
  dia_entrega_label?: string | null
}): string {
  const base =
    row.dia_entrega_label?.trim() ||
    formatDeliveryDayLabel(row.dia_entrega_detectado)
  const obs = (row.observaciones || "").toLowerCase()
  if (/\bretiro\b/.test(obs) && base !== "Sin día") {
    return `Retiro · ${base}`
  }
  if (/\bretiro\b/.test(obs) && base === "Sin día") {
    return "Retiro"
  }
  return base
}
