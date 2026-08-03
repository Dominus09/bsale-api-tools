/** Tipos de la API read-only Costos V2 (montos como string Decimal). */

export type CostV2QualityStatus =
  | "missing_cost"
  | "gross_component_mismatch"
  | "duplicated_taxes_in_gross"
  | "missing_taxes_in_gross"
  | "incomplete_tax_context"
  | "valid_gross"
  | string

export type CostV2WarningCode =
  | "suspicious_outlier"
  | "tax_ids_not_consumed"
  | "variant_barcode_mismatch"
  | "source_conflict"
  | "reception_tax_context_unavailable"
  | "stored_components_rounding"
  | string

export type CostV2AdditionalTax = {
  tax_id: number
  name?: string | null
  rate: string
  category: string
  amount: string
  source?: string | null
}

export type CostV2ReceptionListItem = {
  history_id: number
  company_id: number | null
  office_id: number | null
  admission_date: string | null
  document_number: number | string | null
  document: string | null
  reception_id: number | null
  variant_id: number | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  stored_cost_net: string | null
  stored_cost_gross: string | null
  stored_iva_amount: string | null
  stored_other_taxes: string | null
  stored_quantity: string | null
  corrected_gross_cost: string | null
  calculated_iva_amount: string | null
  additional_tax_amount_total: string | null
  total_tax_rate: string | null
  resolved_tax_ids: unknown[]
  additional_taxes: CostV2AdditionalTax[]
  tax_ids_source: string | null
  tax_rates_source: string | null
  tax_context_quality: string | null
  historical_tax_context_available: boolean | null
  effective_quality_status: CostV2QualityStatus | null
  warnings: CostV2WarningCode[]
  suspicious_outlier: boolean
  calculation_version: string | null
  calculation_batch_id: string | null
  calculated_at: string | null
  source_history_fingerprint: string | null
  tax_context_fingerprint: string | null
  calculation_result_fingerprint: string | null
  unit_difference: string | null
}

export type CostV2CalculationBlock = {
  stored_cost_net: string | null
  iva: {
    tax_id: number | null
    rate: string | null
    amount: string | null
  }
  additional_taxes: CostV2AdditionalTax[]
  corrected_gross_cost: string | null
  formula: string
}

export type CostV2ReceptionDetail = CostV2ReceptionListItem & {
  calculation: CostV2CalculationBlock
  reception_tax_ids: unknown[]
  catalog_tax_ids: unknown[]
  tax_context_source: string | null
  gross_difference_amount: string | null
}

export type CostV2PageInfo = {
  limit: number
  has_more: boolean
  next_cursor: string | null
}

export type CostV2Meta = {
  data_source: string
  calculation_version: string
  latest_view_note?: string
}

export type CostV2ReceptionsResponse = {
  items: CostV2ReceptionListItem[]
  page: CostV2PageInfo
  meta: CostV2Meta
}

export type CostV2ReceptionDetailResponse = {
  item: CostV2ReceptionDetail
  meta: CostV2Meta
}

export type CostV2SummaryBody = {
  total_rows: number
  unique_variants: number
  unique_documents: number
  by_status: Record<string, number>
  by_warning: Record<string, number>
  with_corrected_gross: number
  without_corrected_gross: number
  min_admission_date: string | null
  max_admission_date: string | null
  status_sum_matches_total: boolean
}

export type CostV2SummaryResponse = {
  summary: CostV2SummaryBody
  meta: CostV2Meta
}

export type CostV2ListParams = {
  company_id: number
  office_id: number
  date_from: string
  date_to: string
  status?: string | null
  warning?: string | null
  barcode?: string | null
  search?: string | null
  limit?: number
  cursor?: string | null
  signal?: AbortSignal
}

export type CostV2DetailParams = {
  company_id: number
  office_id: number
  history_id: number
  signal?: AbortSignal
}

export type CostV2SummaryParams = {
  company_id: number
  office_id: number
  date_from: string
  date_to: string
  status?: string | null
  warning?: string | null
  barcode?: string | null
  search?: string | null
  signal?: AbortSignal
}

export const COST_V2_MAX_LIMIT = 200
export const COST_V2_DEFAULT_LIMIT = 50
export const COST_V2_DEFAULT_OFFICE_ID = 3

export type CostV2ProductItem = {
  variant_id: number
  company_id: number | null
  office_id: number | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  latest_history_id: number | null
  latest_admission_date: string | null
  latest_document_number: number | string | null
  latest_document?: string | null
  current_stored_cost_net: string | null
  current_corrected_gross_cost: string | null
  current_stored_gross_cost?: string | null
  current_calculated_iva_amount?: string | null
  current_additional_tax_amount_total?: string | null
  current_additional_taxes?: CostV2AdditionalTax[]
  current_total_tax_rate: string | null
  current_iva_rate?: string | null
  current_quality_status: CostV2QualityStatus | null
  current_warnings: CostV2WarningCode[]
  previous_history_id: number | null
  previous_admission_date: string | null
  previous_corrected_gross_cost: string | null
  unit_change_amount: string | null
  unit_change_percent: string | null
  receptions_count: number
  last_calculated_at: string | null
  needs_review?: boolean
  tax_ids_source?: string | null
  tax_rates_source?: string | null
  tax_context_source?: string | null
  calculation_version?: string | null
  source_history_fingerprint?: string | null
  tax_context_fingerprint?: string | null
  calculation_result_fingerprint?: string | null
  receptions?: CostV2ReceptionListItem[]
  calculation?: CostV2CalculationBlock
}

export type CostV2ProductsResponse = {
  items: CostV2ProductItem[]
  page: CostV2PageInfo & { sort?: string }
  meta: CostV2Meta
}

export type CostV2ProductDetailResponse = {
  item: CostV2ProductItem
  meta: CostV2Meta
}

export type CostV2ProductsSummaryBody = {
  total_products: number
  products_with_current_cost: number
  products_without_calculable_cost: number
  products_incomplete_tax_context: number
  products_with_outlier: number
  products_with_increase: number
  products_with_decrease: number
  products_with_change_over_threshold: number
  products_needing_review: number
  products_missing_cost: number
  products_rounding_warning: number
  latest_reception_date: string | null
  latest_calculation_at: string | null
  change_threshold_percent: string | null
}

export type CostV2ProductsSummaryResponse = {
  summary: CostV2ProductsSummaryBody
  meta: CostV2Meta
}

export type CostV2ProductListParams = {
  company_id: number
  office_id: number
  date_from: string
  date_to: string
  status?: string | null
  warning?: string | null
  barcode?: string | null
  search?: string | null
  sort?: string | null
  only_with_changes?: boolean
  only_needs_review?: boolean
  min_abs_change_percent?: string | number | null
  limit?: number
  cursor?: string | null
  signal?: AbortSignal
}

/** Lectura consolidada por empresa (E.7.3). */
export type CompanyOfficeRow = {
  office_id: number
  office_name: string
  current_cost: string | null
  admission_date: string | null
  history_id: number | null
  quality_status: CostV2QualityStatus | null
  warnings: CostV2WarningCode[]
  diff_vs_company: string | null
  has_v2_data: boolean
  situation: string
  situation_label: string
}

export type CompanyProductItem = {
  variant_id: number
  company_id: number | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  current_history_id: number | null
  current_cost: string | null
  current_cost_raw: string | null
  current_admission_date: string | null
  current_office_id: number | null
  current_office_name: string | null
  current_document_number: number | string | null
  current_quality_status: CostV2QualityStatus | null
  current_warnings: CostV2WarningCode[]
  current_stored_cost_net: string | null
  current_stored_gross_cost: string | null
  current_calculated_iva_amount: string | null
  current_additional_tax_amount_total: string | null
  current_additional_taxes: CostV2AdditionalTax[]
  current_total_tax_rate: string | null
  previous_distinct_history_id: number | null
  previous_distinct_cost: string | null
  change_amount: string | null
  change_percent: string | null
  last_change_date: string | null
  has_comparable_cost: boolean
  visual_no_change: boolean
  active_offices_count: number
  offices_with_v2_data: number
  offices_with_current_cost: number
  coverage_label: string
  requires_review: boolean
  has_office_difference: boolean
  office_alignment_status: string
  business_statuses: string[]
  last_reception_date: string | null
  receptions_in_period: number
  last_calculated_at: string | null
  latest_history_id: number | null
  tax_ids_source: string | null
  tax_rates_source: string | null
  tax_context_source: string | null
  calculation_version: string | null
  calculation_batch_id: string | null
  source_history_fingerprint: string | null
  tax_context_fingerprint: string | null
  calculation_result_fingerprint: string | null
  resolved_tax_ids: unknown[]
  offices?: CompanyOfficeRow[]
}

export type CompanySummary = {
  total_products: number
  products_with_current_cost: number
  products_without_current_cost: number
  relevant_changes: number
  products_requiring_review: number
  products_with_outlier: number
  products_with_office_difference: number | null
  office_difference_comparable: boolean
  active_offices_count: number
  offices_with_v2_coverage: number
  coverage_label: string
  latest_reception_date: string | null
  latest_sync_or_calculation_at: string | null
  change_threshold_percent: string | null
  active_offices: { office_id: number; office_name: string }[]
}

export type CompanyHistoryItem = CostV2ReceptionListItem & {
  office_name: string | null
  prev_cost_in_series: string | null
  cost_changed: boolean
}

export type CompanyProductsResponse = {
  items: CompanyProductItem[]
  page: CostV2PageInfo & { sort?: string }
  meta: CostV2Meta
}

export type CompanyProductResponse = { item: CompanyProductItem; meta: CostV2Meta }
export type CompanySummaryResponse = { summary: CompanySummary; meta: CostV2Meta }
export type CompanyHistoryResponse = { items: CompanyHistoryItem[]; meta: CostV2Meta }

export type CompanyProductListParams = {
  company_id: number
  date_from: string
  date_to: string
  search?: string | null
  barcode?: string | null
  warning?: string | null
  movement?: "up" | "down" | "flat" | null
  situation?: "requires_review" | "office_difference" | "partial_coverage" | null
  only_relevant_changes?: boolean
  min_abs_change_percent?: string | number | null
  sort?: string | null
  limit?: number
  cursor?: string | null
  signal?: AbortSignal
}
