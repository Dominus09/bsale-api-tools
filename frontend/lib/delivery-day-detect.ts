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
