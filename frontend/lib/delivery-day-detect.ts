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

/**
 * Badge semántico por día: cada valor define borde, fondo Y texto en claro
 * y en oscuro (WCAG AA: texto claro 200 sobre fondo 950 en dark; texto
 * 800/900 sobre fondo 50 en light). Nunca dejar el texto light (p. ej.
 * violet-900) sobre el fondo dark: era el defecto «Viernes» ilegible.
 */
const DAY_BADGE_CLASS: Record<DeliveryDayToken, string> = {
  lunes: "border-slate-300 bg-slate-50 text-slate-800 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-200",
  martes: "border-slate-300 bg-slate-50 text-slate-800 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-200",
  miercoles: "border-blue-200 bg-blue-50 text-blue-900 dark:border-blue-800 dark:bg-blue-950/50 dark:text-blue-200",
  jueves: "border-indigo-200 bg-indigo-50 text-indigo-900 dark:border-indigo-800 dark:bg-indigo-950/50 dark:text-indigo-200",
  viernes: "border-violet-200 bg-violet-50 text-violet-900 dark:border-violet-800 dark:bg-violet-950/50 dark:text-violet-200",
  sabado: "border-amber-300 bg-amber-50 text-amber-950 dark:border-amber-800 dark:bg-amber-950/50 dark:text-amber-200",
  domingo: "border-rose-200 bg-rose-50 text-rose-900 dark:border-rose-800 dark:bg-rose-950/50 dark:text-rose-200",
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
