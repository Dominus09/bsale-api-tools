import type { CostBasis, MarginRisk, MarginRule, MarginStatus, MarginsMeta } from "./margins-types"
import type { AppliedTax, CostReliability, Money, TaxProfileStatus } from "./types"

export type PackCostStatus =
  | "calculado"
  | "sin_composicion"
  | "composicion_no_interpretable"
  | "pack_abierto"
  | "ciclo_detectado"
  | "componente_inexistente"
  | "componente_pack_no_calculable"
  | "componente_requiere_revision_tributaria"
  | "componente_sin_costo"

export type PackComponentStatus =
  | "calculado"
  | "inexistente_o_retirado"
  | "sin_costo"
  | "requiere_revision_tributaria"
  | "pack_no_calculable"
  | "ciclo"
  | "anidamiento_excesivo"
  | "variante_no_determinada"

export type PackProblem =
  | "precio_bajo_costo"
  | "bajo_politica"
  | "pack_no_calculable"
  | "componentes_faltantes"
  | "componentes_sin_costo"
  | "componente_costo_desactualizado"
  | "componente_costo_requiere_revision"
  | "componente_alza_de_costo"
  | "componente_inactivo"
  | "detalle_inactivo_excluido"
  | "diferencia_extrema_entre_listas"

export type PackCostCompact = {
  status: PackCostStatus
  calculable: boolean
  basis: CostBasis
  components_count: number
  net_total: Money
  gross_total: Money
  reliability: CostReliability | null
  nested: boolean
  open: boolean
  missing_cost_variant_ids: number[]
  missing_variant_ids: number[]
  composition_issues: string[]
  warnings: string[]
}

export type PackLine = {
  position: number
  detail_id: number | null
  variant_id: number | null
  product_id: number | null
  quantity: Money
  multiple_variant: boolean
  status: PackComponentStatus
  code: string | null
  name: string | null
  active: boolean | null
  is_pack: boolean
  reliability: CostReliability | null
  cost_change_pct: Money
  unit_net: Money
  unit_gross: Money
  subtotal_net: Money
  subtotal_gross: Money
  tax_ids: number[]
  taxes_applied: AppliedTax[]
  tax_factor: Money
  tax_status: TaxProfileStatus | null
  nested_pack?: PackCostDetail
}

export type PackCostDetail = PackCostCompact & {
  source: string
  pack_own_cost_used: false
  pack_own_taxes_used: false
  lines: PackLine[]
  composition_fetched_at?: string | null
}

export type PackRow = {
  variant_id: number
  product_id: number | null
  code: string | null
  barcode?: string | null
  product_name: string | null
  variant_name: string | null
  product_type?: { id: number | null; name: string | null }
  pack_state: "activo" | "inactivo"
  pack_status: PackCostStatus | null
  components_count: number
  price_list: { id: number; name: string | null; label: string | null }
  price_gross: Money
  price_issue: string | null
  cost_net_total: Money
  cost_gross_total: Money
  markup_pct: Money
  margin_on_sale_pct: Money
  margin_status: MarginStatus
  qualifiers: MarginStatus[]
  risks: MarginRisk[]
  reliability: CostReliability
  validity: { status: "provisional" | "validado"; reasons: string[] }
  rule: MarginRule | null
  promotion_check: string
  missing_cost_variant_ids: number[]
  missing_variant_ids: number[]
  problems: PackProblem[]
  updated_at: { price_fetched_at: string | null; composition_fetched_at: string | null }
}

type PriceListRef = { id: number; name: string | null; label: string | null }
type PacksMeta = MarginsMeta & { historical_profitability: "no_implementada" }

export type PackRowsResponse = {
  items: PackRow[]
  page: number
  page_size: number
  total: number
  total_pages: number
  sort: string
  cost_basis: CostBasis
  price_lists: PriceListRef[]
  meta: PacksMeta
}

export type PackListSpread = {
  prices: Record<string, Money>
  min: Money
  max: Money
  ratio: Money
  alert: boolean
  threshold_ratio: Money
  note: string
}

export type PackDetailResponse = {
  variant_id: number
  product_id: number | null
  code: string | null
  product_name: string | null
  variant_name: string | null
  pack_state: "activo" | "inactivo"
  pack: PackCostDetail
  pack_sku_own: {
    bsale_average_cost: { value: Money; fetched_at: string | null; source: string }
    tax_ids: number[] | null
    used_for_cost: false
  }
  rows: PackRow[]
  list_price_spread: PackListSpread | null
  problems: PackProblem[]
  cost_basis: CostBasis
  meta: PacksMeta
}

export type PacksSummary = {
  packs_total: number
  packs_active: number
  packs_active_calculable: number
  packs_active_pending: number
  coverage_pct: string | null
  by_pack_status: Record<PackCostStatus, number>
  combinations_by_margin_status: Record<MarginStatus, number>
  packs_by_problem: Record<PackProblem, number>
  top_below_cost: PackRow[]
  top_below_policy: PackRow[]
  composition_fetched: { min: string | null; max: string | null }
  cost_basis: CostBasis
  price_lists: PriceListRef[]
  meta: PacksMeta
}

export type PackRowsParams = {
  company_id: number
  q?: string
  price_list_id?: number | null
  pack_status?: PackCostStatus | null
  margin_status?: MarginStatus | null
  problem?: PackProblem | null
  pack_state?: "active" | "inactive" | "all"
  cost_basis?: CostBasis
  sort?: string
  page?: number
  page_size?: number
}
