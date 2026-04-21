/**
 * Extrae etiquetas de día / entrega / retiro desde textos de observaciones (es-CL).
 */

/** Coincide sobre texto ya sin tildes (``stripAccents``). */
const WEEKDAY_RE = /(lunes|martes|miercoles|jueves|viernes|sabado)/gi

const TOKEN_TO_LABEL: Record<string, string> = {
  lunes: "Lunes",
  martes: "Martes",
  miercoles: "Miércoles",
  jueves: "Jueves",
  viernes: "Viernes",
  sabado: "Sábado",
}

function stripAccents(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
}

function dayLabelFromMatch(raw: string): string {
  const k = stripAccents(raw)
  return TOKEN_TO_LABEL[k] ?? raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase()
}

/** Etiquetas únicas inferidas de un texto de observación (una OC puede aportar varias). */
export function tagsFromObservationText(text: string): string[] {
  const norm = stripAccents(text)
  if (!norm.trim()) return []
  const found = new Set<string>()
  let m: RegExpExecArray | null
  const re = new RegExp(WEEKDAY_RE.source, "gi")
  while ((m = re.exec(norm)) !== null) {
    const raw = m[1]
    const day = dayLabelFromMatch(raw)
    const start = Math.max(0, m.index - 48)
    const end = Math.min(norm.length, m.index + m[0].length + 48)
    const win = norm.slice(start, end)
    let tag: string
    if (/\b(entrega|reparto|despacho)\b/.test(win)) {
      tag = `Entrega ${day}`
    } else if (/\bretiro\b/.test(win)) {
      tag = `${day} retiro`
    } else {
      tag = `Mención ${day}`
    }
    found.add(tag)
  }
  return Array.from(found)
}

/** Token ASCII para filtro backend (``LIKE %token%`` sobre observaciones normalizadas). */
export function weekdayTokenFromTagLabel(tag: string): string | null {
  const norm = stripAccents(tag)
  if (!norm.trim()) return null
  const re = new RegExp(WEEKDAY_RE.source, "i")
  const m = re.exec(norm)
  if (!m?.[1]) return null
  return stripAccents(m[1]).toLowerCase()
}

export function aggregateObservationTags(texts: readonly string[]): {
  tag: string
  count: number
}[] {
  const counts = new Map<string, number>()
  for (const t of texts) {
    const tags = tagsFromObservationText(t)
    for (const tag of tags) {
      counts.set(tag, (counts.get(tag) ?? 0) + 1)
    }
  }
  return Array.from(counts.entries())
    .map(([tag, count]) => ({ tag, count }))
    .sort((a, b) => b.count - a.count || a.tag.localeCompare(b.tag, "es"))
}
