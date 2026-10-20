/** Tipos del Catálogo Maestro (Inteligencia Comercial, solo lectura). */

export type CatalogPrice = {
  gross: string | null
  fetched_at: string | null
}

export type CatalogTax = {
  tax_id: number
  name: string
  code: string | null
  kind: string
  percentage: string | null
}

export type CatalogRow = {
  company_id: number
  product_id: number | null
  variant_id: number
  product_type: string | null
  product_type_id: number | null
  product_name: string | null
  variant_name: string | null
  barcode: string | null
  code: string | null
  active: boolean
  iva_pct: string | null
  ila_pct: string | null
  taxes: CatalogTax[]
  tax_profile: { status: string; validation: string; reason: string }
  average_cost_net: string | null
  cost_state: "con_costo" | "cero" | "ausente" | "negativo"
  cost_fetched_at: string | null
  prices: Record<string, CatalogPrice>
  missing_price_lists: number[]
  integrity_issues: string[]
  integrity_status: "completo" | "con_observaciones"
}

export type CatalogMeta = {
  company_id: number
  company_name: string | null
  total_variants: number
  price_lists: { id: number; name: string | null; active: boolean }[]
  product_types: { id: number; name: string | null }[]
  freshness: Record<string, string | null>
  source: string
}

export type CatalogRowsResponse = {
  items: CatalogRow[]
  total: number
  page: number
  page_size: number
  total_pages: number
  price_list_ids: number[]
  meta: CatalogMeta
}

export type CatalogParams = {
  company_id: number
  q?: string | null
  product_type_id?: number | null
  state?: string
  cost?: string
  tax?: string
  price_list_id?: number | null
  price_presence?: string | null
  price_list_ids?: string | null
  sort?: string
  page?: number
  page_size?: number
}
