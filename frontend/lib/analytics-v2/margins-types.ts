import type { PackCostCompact } from "./packs-types"
import type { AppliedTax, CostReliability, GrossCostStatus, Money, TaxProfileStatus, TaxValidation } from "./types"

export type MarginStatus =
  | "bajo_politica"
  | "dentro_politica"
  | "sobre_politica"
  | "sin_costo"
  | "sin_precio"
  | "sin_regla"
  | "costo_no_verificado"
  | "requiere_revision_tributaria"
  | "pack_no_calculable"
  | "politica_provisional"
  | "promocion_vigente"
  | "remate_vigente"
  | "promocion_pendiente_verificacion"

export type CostBasis = "ultimo_comparable" | "promedio_bsale"

export type PromotionCheck =
  | "fuente_no_integrada"
  | "sin_excepcion"
  | "excepcion_verificada"
  | "excepcion_no_verificada"
  | "excepcion_pendiente_aprobacion"
  | "conflicto_promociones"
  | "precio_no_verificable"
  | "promocion_legacy_no_verificable"

export type MarginRisk =
  | "venta_bajo_costo"
  | "costo_sospechoso"
  | "costo_desactualizado"
  | "costo_no_verificado"
  | "sin_costo_valido"
  | "precio_no_verificado"

export type PromotionResolution = {
  check: PromotionCheck
  reason: string | null
  exception: {
    kind: "promocion" | "remate"
    origin: "promociones_v2" | "promociones_legacy"
    campaign_id: number | null
    item_id: number | null
    campaign_name: string | null
    starts_on: string | null
    ends_on: string | null
    applicable_gross_price: string | null
    approval: string
    approved_by: string | null
    approved_at: string | null
    reference: string | null
  } | null
  conflicts: unknown[]
  applies_to_this_item?: boolean
}

export type RuleIssue =
  | "maximo_cero_requiere_revision"
  | "minimo_mayor_que_maximo"
  | "regla_inactiva"
  | "minimo_nulo"

export type PriceListScopeStatus =
  | "evaluada"
  | "autorizada_no_vigente"
  | "retirada"
  | "retirada_reactivada"
  | "vigente_no_autorizada"
  | "no_vigente"

export type MarginsMeta = {
  contract_version: string
  data_source: string
  company_id: number
  company_name: string
  generated_at: string | null
  costs_generated_at: string | null
  policies_reviewed: boolean
  net_cost_verified: boolean
  tax_config_verified: boolean
  promotions_integrated: boolean
  evaluated_price_list_ids: number[]
  scope_discrepancies: number
}

export type MarginRule = {
  rule_id: number
  rule_key: string | null
  price_list_id: number
  product_type_id: number | null
  match: "tipo_producto" | "general_lista" | null
  min_markup_pct: Money
  max_markup_pct: Money
  active: boolean
  review_status: "pendiente_revision" | "validada"
  issues: RuleIssue[]
}

export type MarginRow = {
  variant_id: number
  product_id: number | null
  code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  variant_state: string
  product_type: { id: number | null; name: string | null }
  is_pack: boolean
  pack: PackCostCompact | null
  price_list: { id: number; name: string | null; label: string | null }
  status: MarginStatus
  confirmation: "calculado" | "confirmado"
  qualifiers: MarginStatus[]
  risks: MarginRisk[]
  reasons: string[]
  validity: { status: "provisional" | "validado"; reasons: string[] }
  price: { gross: Money; net: Money; issue: string | null; fetched_at: string | null; source: string }
  costs: {
    bsale_average_net: Money
    last_reception_net: Money
    reference: {
      basis: CostBasis
      net: Money
      gross: Money
      gross_status: GrossCostStatus
      gross_reason: string | null
      tax_status: TaxProfileStatus | "por_componente"
      tax_validation: TaxValidation | "pack_no_calculable"
      iva_rate_pct: Money
      ila_rate_pct: Money
      tax_rate_pct: Money
      tax_factor: Money
      tax_ids: number[]
      taxes_applied: AppliedTax[]
      tax_source: string
    }
    validated_gross: { value: Money; status: string }
    reliability: CostReliability
    anomaly_codes: string[]
  }
  markup_pct: Money
  margin_on_sale_pct: Money
  gap_to_min_pct: Money
  rule: MarginRule | null
  target_price: { min: Money; max: Money; provisional: boolean }
  promotion: PromotionResolution
}

export type MarginRowsResponse = {
  items: MarginRow[]
  page: number
  page_size: number
  total: number
  total_pages: number
  sort: string
  cost_basis: CostBasis
  meta: MarginsMeta
}

export type PriceListScopeEntry = {
  price_list_id: number
  name: string | null
  commercial_label: string | null
  raw_state: number | null
  raw_missing: boolean
  in_raw: boolean
  status: PriceListScopeStatus
  discrepancy: boolean
}

export type PriceListScope = {
  company_id: number
  evaluated_price_list_ids: number[]
  price_lists: PriceListScopeEntry[]
  discrepancies: PriceListScopeEntry[]
}

export type PolicyCounts = {
  rules_total: number
  rules_in_scope: number
  rules_out_of_scope: number
  rules_pending_review: number
  rules_max_zero_in_scope: number
  rules_with_issues_in_scope: number
  missing_rule_combinations: number
}

export type MarginsSummary = {
  rows_total: number
  by_status: Record<MarginStatus, number>
  by_qualifier: Partial<Record<MarginStatus, number>>
  by_price_list: {
    price_list_id: number
    name: string | null
    label: string | null
    rows: number
    by_status: Record<MarginStatus, number>
    by_qualifier: Partial<Record<MarginStatus, number>>
    confirmed: number
    provisional: number
  }[]
  policy: PolicyCounts
  scope: PriceListScope
  cost_basis: CostBasis
  meta: MarginsMeta
}

export type MarginsFilters = {
  price_lists: { id: number; name: string | null; label: string | null }[]
  product_types: { id: number; name: string | null; state: number | null }[]
  statuses: MarginStatus[]
  cost_bases: CostBasis[]
  reliability_statuses: CostReliability[]
  sorts: string[]
  meta: MarginsMeta
}

export type PolicyReviewRule = {
  rule_id: number
  rule_key: string | null
  price_list_id: number
  price_list_name: string | null
  price_list_scope: PriceListScopeStatus
  in_commercial_scope: boolean
  product_type_id: number | null
  product_type_name: string | null
  min_markup_pct: Money
  max_markup_pct: Money
  active: boolean
  review_status: "pendiente_revision" | "validada"
  issues: RuleIssue[]
  alerts_enabled: boolean
}

export type PolicyReviewResponse = {
  rules: PolicyReviewRule[]
  missing_rules: {
    price_list_id: number
    price_list_name: string | null
    product_type_id: number | null
    product_type_name: string | null
    priced_active_skus: number
  }[]
  policy: PolicyCounts
  scope: PriceListScope
  meta: MarginsMeta
}

export type MarginRowsParams = {
  company_id: number
  q?: string
  price_list_id?: number | null
  product_type_id?: number | null
  status?: MarginStatus | null
  qualifier?: MarginStatus | null
  reliability?: CostReliability | null
  cost_basis?: CostBasis
  variant_state?: "active" | "inactive" | "all"
  sort?: string
  page?: number
  page_size?: number
}
