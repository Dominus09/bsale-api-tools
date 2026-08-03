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
