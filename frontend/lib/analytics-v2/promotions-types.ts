import type { PromotionCheck, PromotionResolution } from "./margins-types"
import type { Money } from "./types"

export type CampaignKind = "oferta" | "remate"
export type CampaignApproval = "borrador" | "pendiente_aprobacion" | "aprobada" | "rechazada" | "cancelada"
export type CampaignTimeline = "programada" | "vigente" | "finalizada" | "cancelada"
export type ClearanceReason = "vencimiento" | "descontinuado" | "dano" | "sobrestock" | "otro"
export type PriceVerification =
  | "coincide_bsale"
  | "no_coincide_bsale"
  | "sin_precio_bsale"
  | "precio_bsale_desactualizado"
  | "vencida_sin_revertir"
  | "no_aplica"
export type ItemExceptionCheck = PromotionCheck | "no_vigente" | "no_participa"
export type PromotionSection = "vigentes" | "remates" | "proximas" | "pendientes" | "historial"
export type CampaignAction = "edit" | "submit" | "approve" | "reject" | "cancel"

export type PromotionCapabilities = {
  view: boolean
  create: boolean
  edit: boolean
  approve: boolean
  cancel: boolean
}

export type PromotionsMeta = {
  contract_version: string
  data_source: string
  company_id: number
  company_name: string
  today: string
  tables_ready: boolean
  capabilities: PromotionCapabilities
  evaluated_price_list_ids: number[]
  price_max_age_days: number
  writes_bsale: false
  sales_measurement: "no_implementado"
}

export type PriceListRef = { id: number; name: string | null; label: string | null; in_scope: boolean }

export type CampaignItem = {
  item_id: number
  campaign_id: number
  campaign_name: string
  kind: CampaignKind
  approval_status: CampaignApproval
  timeline: CampaignTimeline
  starts_on: string
  ends_on: string
  variant_id: number
  product_id: number | null
  sku_code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  price_list: PriceListRef
  normal_gross_price: Money
  normal_price_fetched_at: string | null
  promo_gross_price: Money
  discount_pct: Money
  bsale_current_gross: Money
  bsale_fetched_at: string | null
  verification: PriceVerification
  exception_check: ItemExceptionCheck
  exception: PromotionResolution | null
  clearance_reason: ClearanceReason | null
  reference_stock: Money
  reference_stock_fetched_at: string | null
  notes: string | null
  item_status: "activo" | "retirado"
  legacy_snapshot_id: number | null
  created_by: string | null
  created_at: string | null
  updated_by: string | null
  updated_at: string | null
  removed_by: string | null
  removed_at: string | null
  version: number
}

export type CampaignSummaryRow = {
  id: number
  kind: CampaignKind
  name: string
  starts_on: string
  ends_on: string
  effective_end_on: string
  approval_status: CampaignApproval
  timeline: CampaignTimeline
  responsible: string | null
  notes: string | null
  created_by: string | null
  created_at: string | null
  updated_by: string | null
  updated_at: string | null
  submitted_by: string | null
  submitted_at: string | null
  approved_by: string | null
  approved_at: string | null
  rejected_by: string | null
  rejected_at: string | null
  cancelled_by: string | null
  cancelled_at: string | null
  status_reason: string | null
  legacy_promotion_id: number | null
  version: number
  items_active: number
  products: number
  price_list_ids: number[]
  verification: Partial<Record<PriceVerification, number>>
  exceptions: Partial<Record<ItemExceptionCheck, number>>
  allowed_actions: CampaignAction[]
}

export type HistoryEvent = {
  id: number
  campaign_id: number
  campaign_name?: string | null
  kind?: CampaignKind | null
  item_id: number | null
  product_name?: string | null
  price_list_id?: number | null
  event: string
  previous_status: string | null
  new_status: string | null
  previous_values: Record<string, unknown> | null
  new_values: Record<string, unknown> | null
  reason: string | null
  actor: string | null
  actor_role: string | null
  occurred_at: string | null
}

export type MeasurementWindow = { from: string | null; to: string | null } | null

export type CampaignDetail = CampaignSummaryRow & {
  items: CampaignItem[]
  history: HistoryEvent[]
  measurement: {
    status: "no_implementado"
    planned_days: number
    real_days: number
    real_end_on: string | null
    before: MeasurementWindow
    during: MeasurementWindow
    after: MeasurementWindow
    metrics: null
  }
  meta: PromotionsMeta
}

export type Paged<T> = {
  items: T[]
  page: number
  page_size: number
  total: number
  total_pages: number
  month?: string
  section?: PromotionSection
  meta: PromotionsMeta
}

export type PromotionsSummary = {
  month: string
  month_from: string
  month_to: string
  kpis: {
    campaigns_vigentes: number
    products_on_offer: number
    products_on_clearance: number
    campaigns_finished_month: number
    exceptions_verified: number
    exceptions_pending: number
    campaigns_pending_approval: number
    campaigns_upcoming: number
  }
  month_by_kind: Partial<Record<CampaignKind, number>>
  month_by_approval: Partial<Record<CampaignApproval, number>>
  month_by_timeline: Partial<Record<CampaignTimeline, number>>
  verification_live: Partial<Record<PriceVerification, number>>
  legacy: { promotions: number; active_flag: number }
  meta: PromotionsMeta
}

export type PromotionsFilters = {
  price_lists: { id: number; name: string | null; label: string | null }[]
  kinds: CampaignKind[]
  approval_statuses: CampaignApproval[]
  timelines: CampaignTimeline[]
  clearance_reasons: ClearanceReason[]
  sections: PromotionSection[]
  meta: PromotionsMeta
}

export type LegacySnapshot = {
  snapshot_id: number
  barcode: string | null
  price_list_text: string | null
  regular_price: Money
  sale_price: Money
  discount_pct: Money
  generated_at: string | null
  mapping_status: string
  variant_id: number | null
  price_list_id: number | null
}

export type LegacyPromotion = {
  promotion_id: number
  tipo: string | null
  canal: string | null
  starts_on: string | null
  ends_on: string | null
  active_flag: boolean
  legacy_status: "activa" | "inactiva" | "programada" | "vencida"
  created_at: string | null
  items: number
  snapshots: LegacySnapshot[]
  imported_campaign_id: number | null
  exception_policy: "nunca_otorga_excepcion"
}

export type LegacyResponse = { promotions: LegacyPromotion[]; total: number; meta: PromotionsMeta }

export type ProductSearchItem = {
  variant_id: number
  product_id: number | null
  code: string | null
  barcode: string | null
  product_name: string | null
  variant_name: string | null
  variant_active: boolean
  prices: { price_list: PriceListRef; gross: Money; fetched_at: string | null }[]
}

export type WriteWarning = { code: string; detail: unknown }
export type ItemWriteResponse = { campaign: CampaignDetail; warnings: WriteWarning[] }
