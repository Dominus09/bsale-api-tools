/** Contratos de Analítica V2 (espejo de backend/services/analytics_v2). Montos = string decimal. */

import type { PackCostDetail } from "./packs-types"

export type MovementClass =
  | "factura"
  | "guia_despacho"
  | "nota_credito"
  | "transferencia_interna"
  | "ajuste"
  | "sin_documento"
  | "no_determinado"

export type CostReliability =
  | "consistente"
  | "requiere_revision"
  | "desactualizado"
  | "solo_promedio_bsale"
  | "sin_costo"

export type AnomalyCode =
  | "alza_anormal"
  | "baja_sospechosa"
  | "costo_cero"
  | "costo_nulo"
  | "cantidad_extrema"
  | "cantidad_no_positiva"
  | "recepcion_modificada"
  | "cambio_variante"
  | "detalle_incompleto"
  | "movimiento_no_determinado"
  | "diferencia_promedio_bsale"

export type Severity = "alta" | "media" | "baja"

export type TaxProfileStatus =
  | "no_consultado"
  | "sin_impuestos"
  | "exento_verificado"
  | "iva"
  | "iva_ila"
  | "iva_anticipado"
  | "ila_combinado"
  | "impuesto_no_reconocido"
  | "configuracion_inconsistente"

export type TaxValidation = "calculable" | "requiere_revision_tributaria" | "sin_configuracion"

export type GrossCostStatus =
  | "preliminar_no_verificado"
  | "requiere_revision_tributaria"
  | "sin_configuracion_tributaria"
  | "sin_costo_base"
  | "pack_no_calculable"

export type Money = string | null

export type V2Meta = {
  contract_version: string
  data_source: string
  company_id: number
  company_name: string
  history_start: string
  net_cost_verified: boolean
  tax_config_verified?: boolean
  generated_at: string | null
}

export type Paginated<T> = {
  items: T[]
  page: number
  page_size: number
  total: number
  total_pages: number
}

export type Anomaly = { code: AnomalyCode; severity: Severity; message: string }

export type ReceptionLineRef = {
  reception_id: number
  detail_id: number
  admission_date: string | null
  office_id: number | null
  office_name: string | null
  document: string | null
  document_number: string | null
  movement_class: MovementClass
  cost: Money
  quantity: Money
}

export type TaxEntry = {
  tax_id: number
  name: string
  code: string | null
  percentage: string | null
  kind: "iva" | "ila" | "iva_anticipado" | "desconocido"
  use: "comparacion_comercial" | "anticipado_contable" | "no_clasificado"
  classified_by: "codigo_sii" | "id_confirmado" | "sin_clasificar"
  issues: string[]
}

export type TaxProfile = {
  status: TaxProfileStatus
  validation: TaxValidation
  reason: string
  iva_rate_pct: string | null
  ila_rate_pct: string | null
  /** Anticipo harina/carne: integra total_rate (pagadero); no entra a margin_rate. */
  iva_anticipado_rate_pct?: string | null
  total_rate_pct: string | null
  /** Base de márgenes: solo IVA+ILA. */
  margin_rate_pct?: string | null
  issues: string[]
  taxes: TaxEntry[]
}

export type CommercialGrossCost = {
  net: Money
  iva_rate_pct: Money
  ila_rate_pct: Money
  iva_anticipado_rate_pct?: Money
  /** Factor bruto pagadero (p. ej. 1,31 harina). */
  factor: Money
  /** Bruto total pagadero estimado. */
  gross: Money
  /** Factor / bruto de base de márgenes (sin anticipo). */
  margin_factor?: Money
  margin_gross?: Money
  /** Alias de margin_gross. */
  net_plus_iva_gross?: Money
  status: GrossCostStatus
  reason: string | null
  base: "promedio_bsale" | "componentes_promedio_bsale"
  tax_ids: number[]
  taxes_applied: AppliedTax[]
  margin_taxes_applied?: AppliedTax[]
  confirmed_by_document?: boolean
  source: string
}

export type AppliedTax = {
  tax_id: number
  code: string | null
  percentage: Money
  kind: TaxEntry["kind"]
}

export type TaxBreakdownLine = {
  tax_id: number
  name: string
  label: string
  code: string | null
  role: TaxEntry["kind"]
  percentage: Money
  amount_estimated: Money
  increases_payable_total?: boolean
  in_margin_base?: boolean
  in_commercial_factor: boolean
  use: TaxEntry["use"]
  source: string
  confirmed_by_document: boolean
}

export type TaxBreakdown = {
  net: Money
  lines: TaxBreakdownLine[]
  net_plus_iva_gross?: Money
  margin_gross?: Money
  margin_factor?: Money
  commercial_gross: Money
  payable_gross_total?: Money
  commercial_factor: Money
  payable_factor?: Money
  documentary_total_referential: Money
  documentary_total_confirmed: Money
  disclaimers: string[]
}

export type CostProductItem = {
  variant_id: number
  product_id: number | null
  is_pack: boolean
  pack: PackCostDetail | null
  code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  product_type: { id: number | null; name: string | null }
  variant_state: "activa" | "inactiva" | "desconocido"
  bsale_average_cost: { value: Money; fetched_at: string | null; source: string }
  last_reception: ReceptionLineRef | null
  last_comparable: ReceptionLineRef | null
  previous_comparable: ReceptionLineRef | null
  change: { amount: Money; pct: Money } | null
  tax_profile: TaxProfile
  commercial_gross_cost: CommercialGrossCost
  tax_breakdown?: TaxBreakdown
  gross_cost_preliminary: { value: Money; status: GrossCostStatus; base: string }
  reliability: { status: CostReliability; reason: string }
  anomalies: Anomaly[]
  counts: {
    lines_total: number
    lines_comparable: number
    receptions_total: number
    first_admission_date: string | null
  }
}

export type CostProductsResponse = Paginated<CostProductItem> & { sort: string; meta: V2Meta }
export type CostProductResponse = CostProductItem & { meta: V2Meta }

export type HistoryLine = {
  reception_id: number
  detail_id: number
  admission_date: string | null
  office_id: number | null
  office_name: string | null
  document: string | null
  document_number: string | null
  internal_dispatch_id: number | null
  movement_class: MovementClass
  comparable: boolean
  cost: Money
  quantity: Money
  previous_comparable_cost: Money
  change_pct: Money
  anomalies: Anomaly[]
}

export type HistoryResponse = Paginated<HistoryLine> & {
  series: { date: string | null; cost: Money; office_id: number | null; reception_id: number }[]
  series_truncated: boolean
  variant_id: number
  meta: V2Meta
}

export type AlertItem = {
  variant_id: number | null
  code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  product_type?: { id: number | null; name: string | null }
  reception_id: number
  detail_id: number
  admission_date: string | null
  office_id: number | null
  office_name: string | null
  document: string | null
  document_number: string | null
  movement_class: MovementClass
  cost: Money
  quantity: Money
  previous_comparable_cost: Money
  change_pct: Money
  max_severity: Severity
  anomalies: Anomaly[]
}

export type AlertsResponse = Paginated<AlertItem> & {
  counts_by_code: Partial<Record<AnomalyCode, number>>
  date_from: string
  date_to: string
  meta: V2Meta
}

export type CostSummary = {
  skus_total: number
  skus_active: number
  /** Catálogo completo (sin filtro de stock); referencia GLOBAL en UI. */
  skus_total_global?: number
  skus_active_global?: number
  stock_filter?: "with" | "without" | "all"
  skus_active_with_bsale_average: number
  skus_active_with_comparable_reception: number
  skus_active_by_reliability: Record<CostReliability, number>
  skus_active_by_anomaly: Partial<Record<AnomalyCode, number>>
  skus_active_by_tax_profile: Partial<Record<TaxProfileStatus, number>>
  movements: {
    movement_class: MovementClass
    lines: number
    receptions: number
    comparable_lines: number
    zero_cost_lines: number
    null_cost_lines: number
    first_admission_date: string | null
    last_admission_date: string | null
  }[]
  meta: V2Meta
}

export type ReceptionAnalysisItem = {
  variant_id: number | null
  code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  product_type: { id: number | null; name: string | null }
  reception_id: number
  detail_id: number
  admission_date: string | null
  office_id: number | null
  office_name: string | null
  document: string | null
  document_number: string | null
  movement_class: MovementClass
  quantity: Money
  cost: Money
  previous_cost: Money
  previous_reception_id: number | null
  previous_detail_id: number | null
  previous_admission_date: string | null
  change_amount: Money
  change_pct: Money
  status: string
  status_label: string
  bsale_average_cost: Money
  unit_note: string | null
  comparison_basis: string
  bsale_average_note: string
}

export type ReceptionAnalysisResponse = Paginated<ReceptionAnalysisItem> & {
  date_from: string
  date_to: string
  direction: string
  stock_filter: string
  meta: V2Meta
}

export type CostFilters = {
  product_types: { id: number; name: string | null; state: number | null }[]
  offices: { id: number; name: string | null; state: number | null; is_virtual: number | null }[]
  movement_classes: MovementClass[]
  reliability_statuses: CostReliability[]
  anomaly_codes: AnomalyCode[]
}

export type ReceptionDetail = {
  reception_id: number
  admission_date: string | null
  office_id: number | null
  office_name: string | null
  document: string | null
  document_number: string | null
  internal_dispatch_id: number | null
  movement_class: MovementClass
  details_complete: boolean | null
  details_count: number | null
  missing_since: string | null
  first_seen_at: string | null
  last_changed_at: string | null
  api_fetched_at: string | null
  lines: {
    detail_id: number
    variant_id: number | null
    code: string | null
    barcode: string | null
    product_name: string | null
    variant_name: string | null
    product_type?: { id: number | null; name: string | null }
    quantity: Money
    cost: Money
    missing_since: string | null
    last_changed_at: string | null
  }[]
  events: {
    event_kind: string
    detected_at: string | null
    detected_by: string | null
    header_changed: boolean | null
    details_changed: boolean | null
    added_lines: number | null
    removed_lines: number | null
    cost_changed_detail_ids: number[]
    quantity_changed_detail_ids: number[]
    variant_changed_detail_ids: number[]
  }[]
  meta: V2Meta
}

export type FreshnessResponse = {
  resources: {
    resource: string
    scope: string
    status: string | null
    last_success_at: string | null
    last_attempt_at: string | null
    last_full_reconcile_at: string | null
  }[]
}

export type ProductListParams = {
  company_id: number
  q?: string
  product_type_id?: number | null
  variant_state?: "active" | "inactive" | "all"
  reliability?: CostReliability | null
  tax_status?: TaxProfileStatus | null
  last_movement_class?: MovementClass | null
  anomaly_code?: AnomalyCode | null
  office_id?: number | null
  min_abs_change_pct?: number | null
  kind?: "all" | "individual" | "pack"
  stock_filter?: "with" | "without" | "all" | null
  sort?: string
  page?: number
  page_size?: number
}
