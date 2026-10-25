import type { MarginRisk, MarginStatus, PromotionCheck } from "./margins-types"
import type { PacksSummary } from "./packs-types"
import type { PriceVerification } from "./promotions-types"
import type {
  AlertItem,
  CostReliability,
  GrossCostStatus,
  Money,
  TaxEntry,
  TaxProfileStatus,
  TaxValidation,
} from "./types"

export type DataClass = "confirmado" | "provisional" | "no_disponible"
export type FreshnessStatus = "al_dia" | "atrasada" | "con_error" | "sin_datos"
export type BlockStatus = "ok" | "sin_permiso" | "no_disponible"

export type Block<T> = {
  status: BlockStatus
  reason: string | null
  elapsed_ms: number
  data: T | null
}

export type Count = { combinations: number; skus: number }

export type CompactMarginRow = {
  variant_id: number
  code: string | null
  barcode?: string | null
  product_name: string | null
  variant_name: string | null
  product_type?: { id: number | null; name: string | null }
  price_list: { id: number; name: string | null; label: string | null }
  status: MarginStatus
  data_class: DataClass
  qualifiers: MarginStatus[]
  risks: MarginRisk[]
  price_gross: Money
  cost_reference_gross: Money
  reliability: CostReliability
  markup_pct: Money
  min_markup_pct: Money
  gap_to_min_pct: Money
  promotion_check: PromotionCheck
  campaign_name: string | null
}

export type DashboardMargins = {
  cost_basis: string
  totals: { combinations: number; skus: number; price_lists: number }
  by_status: Record<MarginStatus, Count>
  by_data_class: Record<DataClass, Count>
  by_risk: Record<MarginRisk, Count>
  alerts: Count
  alerts_pending_promotion_verification: Count
  promotion_exceptions: Count
  promotion_exceptions_with_risk: Count
  by_price_list: {
    price_list_id: number
    name: string | null
    label: string | null
    combinations: number
    priced: number
    alerts: number
    within: number
    above: number
    exceptions: number
    unavailable: number
    tax_review: number
    prices_fetched_min: string | null
    prices_fetched_max: string | null
  }[]
  top_alerts: CompactMarginRow[]
  top_exception_risks: CompactMarginRow[]
  policy: {
    rules_total: number
    rules_in_scope: number
    rules_out_of_scope: number
    rules_pending_review: number
    rules_max_zero_in_scope: number
    rules_with_issues_in_scope: number
    missing_rule_combinations: number
  }
  scope: {
    evaluated: { price_list_id: number; name: string | null; label: string | null }[]
    excluded: { price_list_id: number; name: string | null; status: string }[]
    discrepancies: number
  }
  meta: {
    generated_at: string | null
    costs_generated_at: string | null
    policies_reviewed: boolean
    net_cost_verified: boolean
    tax_config_verified: boolean
    promotions_integrated: boolean
  }
}

export type DashboardTaxes = {
  skus_active: number
  products_active: number
  skus_taxes_identified: number
  skus_gross_calculable: number
  skus_without_cost: number
  skus_tax_review: number
  skus_without_tax_config: number
  coverage_pct: { taxes_identified: string | null; gross_calculable: string | null }
  by_tax_status: Record<TaxProfileStatus, number>
  by_validation: Record<TaxValidation, number>
  by_gross_status: Record<GrossCostStatus, number>
  combinations: {
    tax_ids: number[] | null
    taxes: { tax_id: number; name: string; kind: string }[]
    status: TaxProfileStatus
    validation: TaxValidation
    total_rate_pct: string | null
    skus: number
    products: number
  }[]
  anomalies: {
    code: string
    skus: number
    products: number
    examples: {
      variant_id: number
      product_id: number | null
      code: string | null
      product_name: string | null
      tax_ids: number[] | null
      tax_status: TaxProfileStatus
    }[]
  }[]
  catalog: (TaxEntry & { skus_using: number })[]
  cost_base: "promedio_bsale"
  tax_config_verified: boolean
}

export type DashboardCosts = {
  skus_active: number
  skus_by_reliability: Record<CostReliability, number>
  skus_reliable: number
  skus_with_high_anomaly: number
  relevant_increases: {
    threshold_pct: Money
    total: number
    top: {
      variant_id: number
      code: string | null
      barcode?: string | null
      product_name: string | null
      variant_name: string | null
      product_type?: { id: number | null; name: string | null }
      change_pct: Money
      last_comparable: { admission_date: string | null; cost: Money } | null
      previous_comparable: { admission_date: string | null; cost: Money } | null
      reliability: CostReliability
    }[]
  }
  high_alerts: {
    date_from: string
    date_to: string
    total: number
    counts_by_code: Record<string, number>
    top: AlertItem[]
  }
  data_dates: {
    average_cost_fetched_min: string | null
    average_cost_fetched_max: string | null
    last_reception_date: string | null
  }
  net_cost_verified: boolean
}

export type DashboardPromotions = {
  month: string
  tables_ready: boolean
  kpis: {
    campaigns_vigentes: number
    products_on_offer: number
    products_on_clearance: number
    campaigns_finished_month: number
    exceptions_verified: number
    exceptions_pending: number
    campaigns_pending_approval: number
    campaigns_upcoming: number
    items_expired_not_reverted: number
  }
  verification_live: Partial<Record<PriceVerification, number>>
  legacy: { promotions: number; active_flag: number }
  writes_bsale: false
}

export type DashboardPacks = Pick<
  PacksSummary,
  | "packs_active"
  | "packs_active_calculable"
  | "packs_active_pending"
  | "coverage_pct"
  | "by_pack_status"
  | "packs_by_problem"
  | "top_below_cost"
  | "top_below_policy"
>

export type DashboardFreshness = {
  overall: FreshnessStatus
  stale_after_hours: number
  oldest_success_at: string | null
  resources: {
    resource: string
    status: FreshnessStatus
    last_success_at: string | null
    last_attempt_at: string | null
    raw_status: string | null
    scopes: number
  }[]
}

export type DashboardResponse = {
  today: string
  cost_basis: string
  permissions: { costs: boolean; margins: boolean; promotions: boolean }
  reliability: {
    level: DataClass
    raw_freshness: FreshnessStatus
    policies_reviewed: boolean
    net_cost_verified: boolean
    tax_config_verified: boolean
    warnings: string[]
  }
  freshness: Block<DashboardFreshness>
  margins: Block<DashboardMargins>
  costs: Block<DashboardCosts>
  taxes: Block<DashboardTaxes>
  promotions: Block<DashboardPromotions>
  packs: Block<DashboardPacks>
  meta: {
    company_id: number
    company_name: string
    generated_at: string
    cached: boolean
    elapsed_ms?: number
    writes_bsale: false
  }
}
