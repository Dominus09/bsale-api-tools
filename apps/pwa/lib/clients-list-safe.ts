/**
 * Helpers seguros para listado de clientes (online/offline).
 * Evita excepciones de render por snapshots parciales o mal formados.
 */

export type SafeClientCard = {
  customer_key: string
  client_name?: string | null
  fantasy_name?: string | null
  city?: string | null
  address?: string | null
  observations?: string | null
  document_count: number
  amount_total?: number | null
  has_coordinates: boolean
  route_order?: number | null
  visit_status?: string | null
  attention_status?: string | null
  attention_label_es?: string | null
  result_reason?: string | null
  semaphore_color?: string | null
  closure_review_status?: string | null
  admin_review_pending?: boolean
}

/** Filtra entradas inválidas del snapshot/API sin inventar clientes. */
export function normalizeClientCards(raw: unknown): {
  items: SafeClientCard[]
  skipped: number
} {
  if (!Array.isArray(raw)) return { items: [], skipped: 0 }
  const items: SafeClientCard[] = []
  let skipped = 0
  for (const row of raw) {
    if (!row || typeof row !== "object") {
      skipped += 1
      continue
    }
    const r = row as Record<string, unknown>
    const key = r.customer_key
    if (typeof key !== "string" || !key.trim()) {
      skipped += 1
      continue
    }
    const docCount = Number(r.document_count)
    items.push({
      customer_key: key,
      client_name: (r.client_name as string | null | undefined) ?? null,
      fantasy_name: (r.fantasy_name as string | null | undefined) ?? null,
      city: (r.city as string | null | undefined) ?? null,
      address: (r.address as string | null | undefined) ?? null,
      observations: (r.observations as string | null | undefined) ?? null,
      document_count: Number.isFinite(docCount) ? docCount : 0,
      amount_total:
        typeof r.amount_total === "number" && Number.isFinite(r.amount_total)
          ? r.amount_total
          : null,
      has_coordinates: Boolean(r.has_coordinates),
      route_order:
        typeof r.route_order === "number" && Number.isFinite(r.route_order)
          ? r.route_order
          : null,
      visit_status: (r.visit_status as string | null | undefined) ?? null,
      attention_status: (r.attention_status as string | null | undefined) ?? null,
      attention_label_es:
        (r.attention_label_es as string | null | undefined) ?? null,
      result_reason: (r.result_reason as string | null | undefined) ?? null,
      semaphore_color: (r.semaphore_color as string | null | undefined) ?? null,
      closure_review_status:
        (r.closure_review_status as string | null | undefined) ?? null,
      admin_review_pending: Boolean(r.admin_review_pending),
    })
  }
  return { items, skipped }
}

/** Extrae URLs /_next/static referenciadas en un documento HTML. */
export function extractNextStaticAssets(html: string): string[] {
  if (!html) return []
  const found = new Set<string>()
  const re = /(?:src|href)=["'](\/_next\/static\/[^"'\s>#]+)/g
  let m: RegExpExecArray | null
  while ((m = re.exec(html))) {
    found.add(m[1])
  }
  return [...found]
}
