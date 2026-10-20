export type GondolaOffice = { office_id: number; name: string }
export type GondolaProductType = { product_type_id: number; name: string; variant_count: number }
export type GondolaCompany = { company_id: number; name: string }

export type GondolaOptions = {
  company_id: number
  company_name: string
  offices: GondolaOffice[]
  product_types: GondolaProductType[]
  capabilities: { operate: boolean; cancel: boolean }
  tables_ready: boolean
}

export type CatalogItem = {
  variant_id: number
  product_id: number | null
  sku_code: string | null
  bar_code: string | null
  product_name: string
  variant_name: string | null
  product_type_id: number | null
  product_type_name: string | null
  stock: number
  stock_available: number | null
  stock_reserved: number | null
  stock_fetched_at: string | null
  last_entry_at: string | null
  last_entry_movement: string | null
  last_entry_reception_id: number | null
  exhibition_status: "pendiente" | "encontrado" | null
  found_at: string | null
  found_by: string | null
}

export type CatalogResponse = {
  company_id: number
  office_id: number
  product_type_id: number
  session_id: number | null
  total: number
  page: number
  page_size: number
  items: CatalogItem[]
}

export type GondolaSession = {
  id: number
  company_id: number
  office_id: number
  office_name: string | null
  product_type_id: number
  product_type_name: string | null
  status: "en_curso" | "cerrado" | "cancelado"
  created_by: string
  started_at: string
  finished_at: string | null
  finished_by: string | null
  cancelled_at: string | null
  cancelled_by: string | null
  stock_snapshot_at: string
  total_sku: number
  found_count: number
  pending_count: number
  coverage_pct: number
  stock_changed_count: number
}

export type SessionItem = {
  variant_id: number
  product_id: number | null
  sku_code: string | null
  bar_code: string | null
  product_name: string
  variant_name: string | null
  stock_snapshot: number
  stock_available_snapshot: number | null
  stock_at_close: number | null
  stock_current: number
  stock_changed: boolean
  last_entry_at: string | null
  last_entry_movement: string | null
  exhibition_status: "pendiente" | "encontrado"
  found_at: string | null
  found_by: string | null
  found_device: string | null
}

export type ScanRecord = {
  id: number
  bar_code_raw: string
  variant_id: number | null
  result: ScanResult
  device: string | null
  scanned_by: string
  scanned_at: string
}

export type ReplenishmentItem = {
  variant_id: number
  product_id: number | null
  sku_code: string | null
  bar_code: string | null
  product_name: string
  variant_name: string | null
  status: ReplenishmentStatus
  note: string | null
  stock_snapshot: number
  stock_at_close: number | null
  stock_current: number
  stock_changed: boolean
  updated_by: string | null
  updated_at: string | null
}

export type GondolaSessionDetail = GondolaSession & {
  items: SessionItem[]
  recent_scans: ScanRecord[]
  replenishment?: ReplenishmentItem[]
  replenishment_created?: number
}

export type ScanResult =
  | "encontrado"
  | "duplicado"
  | "ambiguo"
  | "sin_coincidencia"
  | "fuera_de_tipo"
  | "stock_cero"
  | "fuera_de_universo"

export type ScanResponse = {
  result: ScanResult
  bar_code: string
  found_at: string | null
  product: {
    variant_id: number
    product_id: number | null
    sku_code: string | null
    bar_code: string | null
    product_name: string
    variant_name: string | null
    product_type_id?: number | null
    stock: number | null
  } | null
  candidates: Array<{
    variant_id: number
    product_id: number | null
    sku_code: string | null
    bar_code: string | null
    product_name: string
    variant_name: string | null
    stock: number | null
  }>
  progress: { total_sku: number; found_count: number; pending_count: number; coverage_pct: number }
}

export type ReplenishmentStatus =
  | "pendiente_buscar"
  | "encontrado_bodega"
  | "repuesto_gondola"
  | "no_encontrado"
