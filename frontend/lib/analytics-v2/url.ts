/** Parámetros de la URL actual (solo cliente). Los valores se validan en cada pantalla y en el backend. */
export function readQuery(): URLSearchParams {
  if (typeof window === "undefined") return new URLSearchParams()
  return new URLSearchParams(window.location.search)
}

export function queryInt(qs: URLSearchParams, key: string): number | null {
  const raw = qs.get(key)
  if (!raw || !/^\d{1,12}$/.test(raw)) return null
  const n = Number(raw)
  return n >= 1 ? n : null
}

export function queryText(qs: URLSearchParams, key: string, max = 100): string | null {
  const raw = qs.get(key)?.trim()
  return raw ? raw.slice(0, max) : null
}

export function queryEnum<T extends string>(qs: URLSearchParams, key: string, allowed: readonly T[]): T | null {
  const raw = qs.get(key)
  return raw && (allowed as readonly string[]).includes(raw) ? (raw as T) : null
}

export function withQuery(path: string, params: Record<string, string | number | null | undefined>): string {
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v !== null && v !== undefined && v !== "") qs.set(k, String(v))
  }
  const s = qs.toString()
  return s ? `${path}?${s}` : path
}
