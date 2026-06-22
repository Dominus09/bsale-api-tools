import { getApiBaseUrl } from "@/lib/api-base"
import { prepareLabelResolveItems } from "@/lib/etiquetas-excel"
import {
  DEFAULT_FETCH_TIMEOUT_MS,
  DISPATCH_PREP_FETCH_TIMEOUT_MS,
  fetchWithTimeout,
  ORS_FETCH_TIMEOUT_MS,
} from "@/lib/fetch-timeout"

const API_URL = getApiBaseUrl()

// Demo mode state - tracks if we're using fallback data
let isDemoMode = false

// Get demo mode state
export function getIsDemoMode(): boolean {
  return isDemoMode
}

// Set demo mode state
function setDemoMode(value: boolean) {
  isDemoMode = value
  if (typeof window !== "undefined") {
    // Store in sessionStorage so it persists across page navigations
    sessionStorage.setItem("demo_mode", value.toString())
  }
}

// Initialize demo mode from sessionStorage
export function initDemoMode() {
  if (typeof window !== "undefined") {
    const stored = sessionStorage.getItem("demo_mode")
    if (stored === "true") {
      isDemoMode = true
    }
  }
}

// Demo fallback credentials
const DEMO_EMAIL = "prueba.q@gmail.com"
const DEMO_PASSWORD = "123456"

// Mock data for demo mode when API is unavailable
const mockCompanies: Company[] = [
  { company_id: 1, name: "Supermercado Quillotana Centro" },
  { company_id: 2, name: "Supermercado Quillotana Norte" },
  { company_id: 3, name: "Distribuidora Quillotana" },
]

const mockMarginSummary: MarginSummary = {
  total_products: 1250,
  low_margin_count: 45,
  ok_count: 890,
  high_margin_count: 280,
  ultra_high_margin_count: 35,
  average_margin: 23.5,
}

const mockMarginAlerts: MarginAlert[] = [
  { id: 1, product_name: "Leche Entera 1L", current_margin: 8.2, expected_margin: 15, alert_type: "LOW_MARGIN" },
  { id: 2, product_name: "Pan de Molde Integral", current_margin: 6.5, expected_margin: 12, alert_type: "LOW_MARGIN" },
  { id: 3, product_name: "Aceite Vegetal 1L", current_margin: 9.1, expected_margin: 14, alert_type: "LOW_MARGIN" },
  { id: 4, product_name: "Arroz Grado 1 1kg", current_margin: 7.8, expected_margin: 13, alert_type: "LOW_MARGIN" },
  { id: 5, product_name: "Azúcar Blanca 1kg", current_margin: 5.2, expected_margin: 11, alert_type: "LOW_MARGIN" },
]

const mockMarginProducts: MarginProduct[] = [
  { id: 1, product_name: "Leche Entera 1L", cost: 850, price: 925, margin: 8.2, status: "LOW_MARGIN", suggested_price: 1000 },
  { id: 2, product_name: "Pan de Molde Integral", cost: 1200, price: 1280, margin: 6.5, status: "LOW_MARGIN", suggested_price: 1370 },
  { id: 3, product_name: "Aceite Vegetal 1L", cost: 2100, price: 2300, margin: 9.1, status: "LOW_MARGIN", suggested_price: 2450 },
  { id: 4, product_name: "Arroz Grado 1 1kg", cost: 980, price: 1060, margin: 7.8, status: "LOW_MARGIN", suggested_price: 1125 },
  { id: 5, product_name: "Coca-Cola 2L", cost: 1500, price: 1890, margin: 20.6, status: "OK", suggested_price: 1890 },
  { id: 6, product_name: "Detergente Omo 3kg", cost: 4200, price: 5490, margin: 23.5, status: "OK", suggested_price: 5490 },
  { id: 7, product_name: "Papel Higiénico 12un", cost: 3800, price: 4990, margin: 23.9, status: "OK", suggested_price: 4990 },
  { id: 8, product_name: "Café Nescafé 170g", cost: 3500, price: 4890, margin: 28.4, status: "HIGH_MARGIN", suggested_price: 4550 },
  { id: 9, product_name: "Chocolates Surtidos", cost: 2800, price: 4290, margin: 34.7, status: "HIGH_MARGIN", suggested_price: 3640 },
  { id: 10, product_name: "Snacks Premium", cost: 1200, price: 2490, margin: 51.8, status: "ULTRA_HIGH_MARGIN", suggested_price: 1560 },
]

const mockProductsWithoutCost: ProductWithoutCost[] = [
  { id: 1, product_name: "Yogurt Natural 1L", sku: "YOG-001", category: "Lácteos" },
  { id: 2, product_name: "Queso Gouda 250g", sku: "QUE-002", category: "Lácteos" },
  { id: 3, product_name: "Jamón Serrano 100g", sku: "JAM-003", category: "Fiambres" },
  { id: 4, product_name: "Vino Tinto Reserva", sku: "VIN-004", category: "Bebidas" },
  { id: 5, product_name: "Galletas Integrales", sku: "GAL-005", category: "Snacks" },
]

export interface LoginResponse {
  token: string
  email: string
  role: string
}

export interface Company {
  company_id: number
  name: string
}

export interface MarginProduct {
  id: number
  product_name: string
  cost: number
  price: number
  margin: number
  status: "LOW_MARGIN" | "OK" | "HIGH_MARGIN" | "ULTRA_HIGH_MARGIN"
  suggested_price: number
}

export interface MarginSummary {
  total_products: number
  low_margin_count: number
  ok_count: number
  high_margin_count: number
  ultra_high_margin_count: number
  average_margin: number
}

export interface MarginAlert {
  id: number
  product_name: string
  current_margin: number
  expected_margin: number
  alert_type: string
}

export interface ProductWithoutCost {
  id: number
  product_name: string
  sku?: string
  category?: string
}

export interface Supplier {
  id: number
  name: string
  contact_name: string | null
  phone: string | null
  email: string | null
  notes: string | null
  payment_method: string | null
  visit_day: string | null
  is_active: boolean
  created_at?: string
  updated_at?: string
}

export interface CreateSupplierPayload {
  name: string
  contact_name?: string | null
  phone?: string | null
  email?: string | null
  notes?: string | null
  payment_method?: string | null
  visit_day?: string | null
}

export interface UpdateSupplierPayload {
  name?: string
  contact_name?: string | null
  phone?: string | null
  email?: string | null
  notes?: string | null
  payment_method?: string | null
  visit_day?: string | null
  is_active?: boolean
}

/** Fila de bsale.vw_purchase_analysis (GET /purchase-analysis) */
export interface PurchaseAnalysisRow {
  company_id: number
  office_id: number
  variant_id: number
  product_type_name: string | null
  product_name: string | null
  variant_name: string | null
  barcode: string | null
  ventas_7_dias: number
  ventas_30_dias: number
  promedio_diario: number
  stock_actual: number
  costo_bruto: number
  /** Cobertura objetivo de la sugerencia (14 días). */
  dias_cobertura: number
  /** demanda_14d ≈ (ventas_30/30)*14 desde la vista. */
  demanda_proyectada: number
  unidades_a_comprar: number
  /** CxC desde bsale.variants.units_per_box; la vista puede completar con SEC en description si columna NULL/0. */
  units_per_box: number | null
  /** Coalesce(units_per_box útil, 1) — base para cajas_sugeridas y reglas de estado. */
  units_per_box_eff: number
  cajas_sugeridas: number
  /** estado_sistema (backend / vista). No usar como etiqueta en UI; el front calcula estado_usuario. */
  status: string
  costo_total_compra: number
}

export interface PurchaseOrderHeader {
  oc_id: number
  company_id: number
  company_name?: string | null
  office_id: number
  /** Nombre desde bsale.offices (sync Bsale); null si no hay fila */
  office_name?: string | null
  /** Bsale offices.state; activa en compras = 0 */
  office_state?: number | null
  supplier_id: number
  supplier_name: string | null
  fecha_emision: string | null
  fecha_entrega: string | null
  total_oc: number
  forma_pago: string | null
  responsable: string | null
  observacion: string | null
  status: string
  created_at?: string | null
}

export type PurchaseDataFreshnessStockStatus = "OK" | "REVISAR" | "DESACTUALIZADO"

export type PurchaseDataFreshnessSalesStatus = "OK" | "ESPERANDO ACTUALIZACIÓN" | "ERROR / NO ACTUALIZADO"

export interface PurchaseDataFreshness {
  company_id: number
  last_stock_update: string | null
  last_sales_update: string | null
  stock: {
    status: PurchaseDataFreshnessStockStatus
    minutes_ago: number | null
    message: string
  }
  sales: {
    status: PurchaseDataFreshnessSalesStatus
    message: string
  }
}

export interface PurchaseOfficeRef {
  office_id: number
  /** Nombre desde bsale.offices (sync Bsale); null si no hay fila */
  office_name: string | null
  /** Bsale offices.state; listado compras solo incluye state = 0 */
  office_state: number | null
  /** true en respuesta de purchase-offices (solo sucursales state = 0) */
  is_active: boolean | null
  /** Texto para mostrar en UI */
  label: string
}

export interface PurchaseLinePayload {
  variant_id?: number | null
  product_type_name?: string | null
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  cantidad: number
  units_per_box?: number | null
  costo_unitario: number
}

export interface GeneratePurchaseOrderFromLinesPayload {
  company_id: number
  office_id: number
  supplier_id: number
  fecha_entrega?: string | null
  forma_pago?: string | null
  responsable?: string | null
  observacion?: string | null
  lines: PurchaseLinePayload[]
}

export interface PurchaseOrderDetailRow {
  oc_detail_id: number
  oc_id: number
  company_id: number
  office_id: number
  variant_id: number | null
  product_type_name: string | null
  product_name: string | null
  variant_name: string | null
  barcode: string | null
  cantidad: number
  units_per_box: number | null
  cajas: number | null
  costo_unitario: number
  costo_total: number
  created_at?: string | null
}

export interface PurchaseManualItem {
  id: number
  company_id: number
  office_id: number
  supplier_id: number
  product_type_name: string | null
  product_name: string | null
  variant_name: string | null
  barcode: string | null
  units_per_box: number | null
  costo_bruto: number | null
  cantidad: number
  oc_id: number | null
  consumed_at: string | null
  created_at?: string | null
}

export interface CreatePurchaseManualItemPayload {
  company_id: number
  office_id: number
  supplier_id: number
  product_type_name?: string | null
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  units_per_box?: number | null
  costo_bruto?: number | null
  cantidad: number
}

export interface GeneratePurchaseOrderPayload {
  company_id: number
  office_id: number
  supplier_id: number
  fecha_emision?: string | null
  fecha_entrega?: string | null
  forma_pago?: string | null
  responsable?: string | null
  observacion?: string | null
  manual_ids?: number[] | null
}

export interface ProductMasterRow {
  id: number
  barcode: string
  sku: string | null
  product_id?: number | null
  variant_id?: number | null
  product_name: string | null
  variant_name: string | null
  product_type: string | null
  supplier_id: number | null
  units_per_box?: number | null
  weight_box_kg?: number | null
  height_cm?: number | null
  width_cm?: number | null
  length_cm?: number | null
  weight_unit_kg?: number | null
  volume_m3?: number | null
  logistics_completed?: boolean
  last_bsale_sync_at?: string | null
  is_active: boolean
  created_at?: string | null
  updated_at?: string | null
}

/** Lista de precios activa (GET /price-lists) */
export interface PriceListRef {
  id: number
  name: string
}

/** Fila de bsale.margin_analysis_view (GET /margin-analysis-view) */
export interface MarginAnalysisViewRow {
  company_id: number
  product_type_id?: number | null
  product_type_name?: string | null
  product_name: string | null
  variant_id: number
  variant_name: string | null
  barcode?: string | null
  sku: string | null
  price_list_id: number
  price_list_name?: string | null
  stock_quantity?: number | string | null
  price: number | string | null
  cost: number | string | null
  margin_value?: number | string | null
  margin_percent: number | string | null
  min_margin_percent: number | string | null
  margin_diff?: number | string | null
  status: string
}

export function getAuthHeaders(): HeadersInit {
  const token = typeof window !== "undefined" ? localStorage.getItem("token") : null
  return {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
}

/** Empresa seleccionada en localStorage (login / contexto dashboard). */
export function getStoredCompanyId(): number | null {
  if (typeof window === "undefined") return null
  const raw = localStorage.getItem("company_id")
  if (!raw) return null
  const n = parseInt(raw, 10)
  return Number.isFinite(n) && n > 0 ? n : null
}

function getCompanyId(): number | null {
  return getStoredCompanyId()
}

// Helper function to check if error is a network error
function isNetworkError(error: unknown): boolean {
  return (
    error instanceof TypeError &&
    (error.message.includes("Failed to fetch") ||
      error.message.includes("NetworkError") ||
      error.message.includes("Network request failed"))
  )
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  try {
    const res = await fetch(`${API_URL}/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    })
    if (!res.ok) {
      throw new Error("Credenciales inválidas")
    }
    setDemoMode(false)
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on login, falling back to demo mode", {
        apiUrl: API_URL,
        origin: typeof window !== "undefined" ? window.location.origin : "(ssr)",
        hint:
          "Fetch vía /api-upstream (rewrite Next) evita CORS; si falla: CSP/Cloudflare, NEXT_PUBLIC_API_URL en build y que el API responda.",
      })
      setDemoMode(true)
      // Allow demo login with specific credentials or any credentials
      if ((email === DEMO_EMAIL && password === DEMO_PASSWORD) || (email && password)) {
        await new Promise((resolve) => setTimeout(resolve, 500))
        return { token: "demo-token-12345", email, role: "admin" }
      }
      throw new Error("Credenciales inválidas")
    }
    throw error
  }
}

export async function getCompanies(): Promise<Company[]> {
  try {
    const res = await fetch(`${API_URL}/companies`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) {
      throw new Error("Error al cargar empresas")
    }
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on getCompanies, using fallback data")
      setDemoMode(true)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return mockCompanies
    }
    throw error
  }
}

export async function getMarginSummary(): Promise<MarginSummary> {
  try {
    const companyId = getCompanyId()
    const res = await fetch(`${API_URL}/margin-summary?company_id=${companyId}`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) {
      throw new Error("Error al cargar resumen de márgenes")
    }
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on getMarginSummary, using fallback data")
      setDemoMode(true)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return mockMarginSummary
    }
    throw error
  }
}

export async function getMarginAlerts(): Promise<MarginAlert[]> {
  try {
    const companyId = getCompanyId()
    const res = await fetch(`${API_URL}/margin-alerts?company_id=${companyId}`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) {
      throw new Error("Error al cargar alertas")
    }
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on getMarginAlerts, using fallback data")
      setDemoMode(true)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return mockMarginAlerts
    }
    throw error
  }
}

export async function getMarginAnalysis(): Promise<MarginProduct[]> {
  try {
    const companyId = getCompanyId()
    const res = await fetch(`${API_URL}/margin-analysis?company_id=${companyId}`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) {
      throw new Error("Error al cargar análisis de márgenes")
    }
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on getMarginAnalysis, using fallback data")
      setDemoMode(true)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return mockMarginProducts
    }
    throw error
  }
}

export async function getPriceLists(
  companyId: number | null | undefined,
): Promise<PriceListRef[]> {
  if (companyId == null) {
    return []
  }
  const id = typeof companyId === "number" ? companyId : Number(companyId)
  if (!Number.isFinite(id) || id <= 0) {
    return []
  }

  const res = await fetch(
    `${API_URL}/price-lists?company_id=${encodeURIComponent(String(id))}`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    throw new Error("Error al cargar listas de precios")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export async function getMarginAnalysisView(
  companyId: number,
  priceListId?: number | null,
): Promise<MarginAnalysisViewRow[]> {
  const qs = new URLSearchParams({ company_id: String(companyId) })
  if (priceListId != null && !Number.isNaN(priceListId)) {
    qs.set("price_list_id", String(priceListId))
  }
  const res = await fetch(`${API_URL}/margin-analysis-view?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar análisis de márgenes (vista)")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export type MarginRuleRow = {
  id: number
  company_id: number
  company_name?: string | null
  price_list_id: number
  price_list_name?: string | null
  product_type_id?: number | null
  product_type_name?: string | null
  min_margin: number
  max_margin: number
  active: boolean
  notes?: string | null
  rule_key: string
}

export async function getMarginRules(params?: {
  company_id?: number
  price_list_id?: number
  product_type_id?: number
  active?: "all" | "active" | "inactive"
}): Promise<{ items: MarginRuleRow[]; count: number }> {
  const qs = new URLSearchParams()
  if (params?.company_id != null) qs.set("company_id", String(params.company_id))
  if (params?.price_list_id != null) qs.set("price_list_id", String(params.price_list_id))
  if (params?.product_type_id != null) qs.set("product_type_id", String(params.product_type_id))
  if (params?.active === "active") qs.set("active", "true")
  else if (params?.active === "inactive") qs.set("active", "false")
  else if (params?.active === "all") qs.set("active", "all")
  const res = await fetch(`${API_URL}/margin-rules?${qs}`, { headers: getAuthHeaders() })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar reglas de margen")
  }
  return res.json() as Promise<{ items: MarginRuleRow[]; count: number }>
}

export async function patchMarginRule(
  ruleId: number,
  body: {
    min_margin: number
    max_margin: number
    active: boolean
    notes?: string | null
  },
): Promise<{ item: MarginRuleRow; warnings?: string[] }> {
  const res = await fetch(`${API_URL}/margin-rules/${ruleId}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al guardar regla")
  }
  return res.json() as Promise<{ item: MarginRuleRow; warnings?: string[] }>
}

export async function downloadMarginRulesExcel(params?: {
  company_id?: number
  price_list_id?: number
  product_type_id?: number
  active?: "all" | "active" | "inactive"
}): Promise<void> {
  const qs = new URLSearchParams()
  if (params?.company_id != null) qs.set("company_id", String(params.company_id))
  if (params?.price_list_id != null) qs.set("price_list_id", String(params.price_list_id))
  if (params?.product_type_id != null) qs.set("product_type_id", String(params.product_type_id))
  if (params?.active === "active") qs.set("active", "true")
  else if (params?.active === "inactive") qs.set("active", "false")
  const res = await fetch(`${API_URL}/margin-rules/export?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al exportar reglas")
  }
  const blob = await res.blob()
  const cd = res.headers.get("Content-Disposition")
  const m = cd?.match(/filename="([^"]+)"/)
  const name = m?.[1] ?? "politica_margenes.xlsx"
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export async function getProductsWithoutCost(): Promise<ProductWithoutCost[]> {
  try {
    const companyId = getCompanyId()
    const res = await fetch(`${API_URL}/products-without-cost?company_id=${companyId}`, {
      headers: getAuthHeaders(),
    })
    if (!res.ok) {
      throw new Error("Error al cargar productos sin costo")
    }
    return res.json()
  } catch (error) {
    if (isNetworkError(error)) {
      console.warn("[API] Network error on getProductsWithoutCost, using fallback data")
      setDemoMode(true)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return mockProductsWithoutCost
    }
    throw error
  }
}

/** Cliente del rutero para mapa (API /distribuidora/mapa). */
export interface DistribuidoraMapaCliente {
  bsale_id: number
  first_name: string | null
  last_name: string | null
  nombre_fantasia: string | null
  phone: string | null
  vendedor: string | null
  /** Día efectivo de ruta (incluye sábado extra vía `dia_extra`). */
  dia_operativo?: string | null
  dia_atencion: string | null
  dia_extra: string | null
  municipality: string | null
  lat: number
  lon: number
  tipo_atencion: string | null
  orden_ruta: number | null
  orden_manual: number | null
}

export interface DistribuidoraPuntoBase {
  vendedor: string | null
  nombre: string | null
  lat: number
  lon: number
}

export async function getDistribuidoraMapa(): Promise<{
  clientes: DistribuidoraMapaCliente[]
  bases: DistribuidoraPuntoBase[]
  /** Días operativos distintos (lun–dom; sábado extra incluido como Sabado). */
  dias_atencion?: string[]
  /** Códigos vendedor normalizados (minúsculas, sin espacios extremos), distintos en rutero activo. */
  vendedores?: string[]
}> {
  const res = await fetch(`${API_URL}/distribuidora/mapa`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar mapa del rutero")
  }
  return res.json()
}

/** Guarda orden de visita manual (bsale.rutero.orden_manual). */
export async function postDistribuidoraOrdenManual(body: {
  cliente_id: number
  orden_manual: number
}): Promise<void> {
  const res = await fetch(`${API_URL}/distribuidora/orden-manual`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo guardar el orden manual")
  }
}

/** Limpia orden_manual para vendedor+día (vuelve a optimización ORS). */
export async function postDistribuidoraOrdenManualReset(body: {
  vendedor: string
  dia: string
}): Promise<void> {
  const res = await fetch(`${API_URL}/distribuidora/orden-manual/reset`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo limpiar el orden manual")
  }
}

/** Respuesta de POST /distribuidora/optimizar-ruta (misma forma que ruta-detalle o `{ error }`). */
export type DistribuidoraOptimizarRutaJson = Record<string, unknown>

export async function postDistribuidoraOptimizarRuta(body: {
  vendedor: string
  dia: string
  /** Primer índice 0-based del tramo a reordenar; visitas 0..k-1 quedan fijas (omitir = toda la ruta). */
  bloque_hasta_indice?: number | null
  /** Minutos de atención por visita para el total real (si no se envía, usa default del servidor). */
  tiempo_por_cliente_min?: number | null
}): Promise<DistribuidoraOptimizarRutaJson> {
  const res = await fetch(`${API_URL}/distribuidora/optimizar-ruta`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo optimizar la ruta")
  }
  return res.json()
}

/** POST /distribuidora/optimizar-ruta-desde — misma forma que optimizar-ruta o `{ error }`. */
export async function postDistribuidoraOptimizarRutaDesde(body: {
  vendedor: string
  dia: string
  desde_indice: number
  tiempo_por_cliente_min?: number | null
}): Promise<DistribuidoraOptimizarRutaJson> {
  const res = await fetch(`${API_URL}/distribuidora/optimizar-ruta-desde`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo reoptimizar la ruta")
  }
  return res.json()
}

export async function postDistribuidoraOrdenManualBulk(
  items: { id: number; orden_manual: number }[],
): Promise<void> {
  const res = await fetch(`${API_URL}/distribuidora/orden-manual-bulk`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(items),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo guardar el orden")
  }
}

/** Respuesta de GET /distribuidora/ruta-detalle (éxito o cuerpo con `error`). */
export type DistribuidoraRutaDetalleJson = Record<string, unknown>

export function isDistribuidoraRutaDetalleOk(
  d: DistribuidoraRutaDetalleJson,
): d is DistribuidoraRutaDetalleJson & {
  vendedor: string
  dia: string
  km_totales: number
  minutos_totales: number
  clientes: unknown[]
} {
  if (!d || typeof d !== "object") return false
  const hasClientes = Array.isArray(d.clientes) && d.clientes.length > 0
  if ("error" in d && d.error && !hasClientes) return false
  if (!("km_totales" in d) || !("minutos_totales" in d) || !("clientes" in d)) return false
  if (!Array.isArray(d.clientes)) return false
  return true
}

export async function getDistribuidoraRutaDetalle(
  vendedor: string,
  dia: string,
  signal?: AbortSignal,
): Promise<DistribuidoraRutaDetalleJson> {
  const qs = new URLSearchParams({ vendedor, dia })
  const res = await fetch(`${API_URL}/distribuidora/ruta-detalle?${qs}`, {
    headers: getAuthHeaders(),
    signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar ruta detalle")
  }
  return res.json()
}

/** GET /distribuidora/ruta-sugerencias — swaps adyacentes que mejoran distancia local (Haversine). */
export type DistribuidoraRutaSugerenciaJson = {
  id: string
  tipo: string
  indice_a: number
  indice_b: number
  orden_visita_a: number
  orden_visita_b: number
  bsale_id_a: number
  bsale_id_b: number
  nombre_a: string
  nombre_b: string
  delta_km: number
  mensaje: string
}

export type DistribuidoraRutaSugerenciasResponse = {
  vendedor: string
  dia: string
  metrica: string
  min_delta_km: number
  sugerencias: DistribuidoraRutaSugerenciaJson[]
  nota?: string
  error?: string
}

export async function getDistribuidoraRutaSugerencias(
  vendedor: string,
  dia: string,
  options?: { minDeltaKm?: number; signal?: AbortSignal },
): Promise<DistribuidoraRutaSugerenciasResponse> {
  const qs = new URLSearchParams({ vendedor, dia })
  if (options?.minDeltaKm != null) qs.set("min_delta_km", String(options.minDeltaKm))
  const res = await fetch(`${API_URL}/distribuidora/ruta-sugerencias?${qs}`, {
    headers: getAuthHeaders(),
    signal: options?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar sugerencias de ruta")
  }
  return res.json() as Promise<DistribuidoraRutaSugerenciasResponse>
}

/** GET /distribuidora/resumen-vendedor — rutas por día para un vendedor. */
export type DistribuidoraResumenDiaJson = {
  dia: string
  color: string
  km_totales: number
  minutos_totales: number
  clientes_count: number
  geometry: unknown
  base: unknown
  clientes: unknown[]
  alerta_calidad?: boolean
  km_por_cliente?: number
}

export type DistribuidoraResumenVendedorJson = {
  vendedor: string
  dias: DistribuidoraResumenDiaJson[]
  km_total_semana: number
  min_total_semana: number
  clientes_total_semana: number
  promedio_km_por_dia: number
  km_dia_mas_largo: number
  km_dia_mas_corto: number
}

export async function getDistribuidoraResumenVendedor(
  vendedor: string,
  signal?: AbortSignal,
): Promise<DistribuidoraResumenVendedorJson> {
  const qs = new URLSearchParams({ vendedor })
  const res = await fetch(`${API_URL}/distribuidora/resumen-vendedor?${qs}`, {
    headers: getAuthHeaders(),
    signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar resumen vendedor")
  }
  return res.json() as Promise<DistribuidoraResumenVendedorJson>
}

/** Fila de GET /distribuidora/orders/purchase (vista enriquecida + seller). */
export type DistribuidoraPurchaseOrder = {
  document_id: number
  number?: number | null
  client_id?: number | null
  user_id?: number | null
  seller_id?: number | null
  /** Vendedor original (sync sellers.json); no depende del ``user_id`` del documento. */
  seller_name?: string | null
  emission_date?: string | null
  total_amount?: number | null
  municipality?: string | null
  city?: string | null
  address?: string | null
  nombre_fantasia?: string | null
  forma_pago?: string | null
  observaciones?: string | null
  is_invoiced?: boolean | null
  /** FACTURADA_CONFIRMADA | PROBABLE_FACTURADA_* | PENDIENTE. */
  purchase_status?: string | null
  estado_real?: string | null
  oc_number?: number | null
  oc_client_name?: string | null
  associated_document_label?: string | null
  display_score?: number | null
  candidate_number?: number | null
  candidate_document_type?: number | null
  candidate_document_type_label?: string | null
  score?: number | null
  match_products_pct?: number | null
  probable_document_id?: number | null
  probable_document_type_id?: number | null
  probable_number?: number | null
  probable_score?: number | null
  probable_tier?: string | null
  invoicing_number?: number | null
  invoicing_document_type_id?: number | null
  /** Nombre a mostrar: prioriza ``seller_name`` en API. */
  seller?: string | null
  [key: string]: unknown
}

export type DistribuidoraOrdersPurchaseResponse = {
  total: number
  limit: number
  offset: number
  items: DistribuidoraPurchaseOrder[]
}

export async function getDistribuidoraOrdersPurchase(params: {
  emission_date_from: string
  emission_date_to: string
  only_not_invoiced?: boolean
  /** confirmed | probable | pending */
  invoice_status?: string
  user_id?: number
  delivery_search?: string
  municipality?: string
  limit?: number
  offset?: number
  signal?: AbortSignal
}): Promise<DistribuidoraOrdersPurchaseResponse> {
  const qs = new URLSearchParams()
  qs.set("emission_date_from", params.emission_date_from)
  qs.set("emission_date_to", params.emission_date_to)
  if (params.only_not_invoiced) qs.set("only_not_invoiced", "true")
  if (params.invoice_status?.trim())
    qs.set("invoice_status", params.invoice_status.trim())
  if (params.user_id != null) qs.set("user_id", String(params.user_id))
  if (params.delivery_search?.trim())
    qs.set("delivery_search", params.delivery_search.trim())
  if (params.municipality?.trim())
    qs.set("municipality", params.municipality.trim())
  qs.set("limit", String(params.limit ?? 5000))
  qs.set("offset", String(params.offset ?? 0))
  const res = await fetch(`${API_URL}/distribuidora/orders/purchase?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar órdenes de compra")
  }
  return res.json() as Promise<DistribuidoraOrdersPurchaseResponse>
}

export type DistribuidoraDispatchPrepMunicipalityRow = {
  municipality: string
  clientes_unicos: number
  pedidos: number
  total_ventas: number
}

export type DistribuidoraDispatchPrepByMunicipalityResponse = {
  items: DistribuidoraDispatchPrepMunicipalityRow[]
}

export async function getDistribuidoraDispatchPrepByMunicipality(params: {
  emission_date_from: string
  emission_date_to: string
  only_not_invoiced?: boolean
  day_filter?: string | null
  /** Máximo de comunas (grupos); default 250, máx. 300 en API. */
  limit?: number
  signal?: AbortSignal
}): Promise<DistribuidoraDispatchPrepByMunicipalityResponse> {
  const qs = new URLSearchParams()
  qs.set("emission_date_from", params.emission_date_from)
  qs.set("emission_date_to", params.emission_date_to)
  if (params.only_not_invoiced === false) qs.set("only_not_invoiced", "false")
  if (params.day_filter?.trim()) qs.set("day_filter", params.day_filter.trim())
  if (params.limit != null) qs.set("limit", String(params.limit))
  const res = await fetch(
    `${API_URL}/distribuidora/orders/dispatch-prep/by-municipality?${qs}`,
    { headers: getAuthHeaders(), signal: params.signal },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar resumen por comuna")
  }
  return res.json() as Promise<DistribuidoraDispatchPrepByMunicipalityResponse>
}

export type DistribuidoraDispatchPrepPaginatedMeta = {
  has_more: boolean
  limit: number
  offset: number
  range_days?: number
  wide_range?: boolean
  warning?: string | null
}

export type DistribuidoraDispatchPrepObservacionesResponse =
  DistribuidoraDispatchPrepPaginatedMeta & {
    items: string[]
  }

export async function getDistribuidoraDispatchPrepObservaciones(params: {
  emission_date_from: string
  emission_date_to: string
  only_not_invoiced?: boolean
  day_filter?: string | null
  limit?: number
  offset?: number
  signal?: AbortSignal
}): Promise<DistribuidoraDispatchPrepObservacionesResponse> {
  const qs = new URLSearchParams()
  qs.set("emission_date_from", params.emission_date_from)
  qs.set("emission_date_to", params.emission_date_to)
  if (params.only_not_invoiced === false) qs.set("only_not_invoiced", "false")
  if (params.day_filter?.trim()) qs.set("day_filter", params.day_filter.trim())
  qs.set("limit", String(params.limit ?? 500))
  if (params.offset != null) qs.set("offset", String(params.offset))
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/orders/dispatch-prep/observaciones?${qs}`,
    { headers: getAuthHeaders(), signal: params.signal },
    DISPATCH_PREP_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar observaciones")
  }
  return res.json() as Promise<DistribuidoraDispatchPrepObservacionesResponse>
}

export type DistribuidoraDispatchPrepPlanningRow = {
  document_id: number
  oc?: number | null
  client_id?: number | null
  nombre_fantasia?: string | null
  municipality?: string | null
  direccion?: string | null
  seller_name?: string | null
  total_amount?: number | null
  has_georef?: boolean | null
  lat?: number | null
  lng?: number | null
  estado_real?: string | null
  purchase_status?: string | null
  associated_document_label?: string | null
  display_score?: number | null
  probable_score?: number | null
  probable_tier?: string | null
  observaciones?: string | null
  dia_entrega_detectado?: string | null
  dia_entrega_label?: string | null
  dia_entrega_fuente?: string | null
}

export type DistribuidoraDispatchPrepPlanningRowsResponse =
  DistribuidoraDispatchPrepPaginatedMeta & {
    items: DistribuidoraDispatchPrepPlanningRow[]
  }

export async function getDistribuidoraDispatchPrepPlanningRows(params: {
  emission_date_from: string
  emission_date_to: string
  only_not_invoiced?: boolean
  day_filter?: string | null
  limit?: number
  offset?: number
  signal?: AbortSignal
}): Promise<DistribuidoraDispatchPrepPlanningRowsResponse> {
  const qs = new URLSearchParams()
  qs.set("emission_date_from", params.emission_date_from)
  qs.set("emission_date_to", params.emission_date_to)
  if (params.only_not_invoiced === false) qs.set("only_not_invoiced", "false")
  if (params.day_filter?.trim()) qs.set("day_filter", params.day_filter.trim())
  qs.set("limit", String(params.limit ?? 500))
  if (params.offset != null) qs.set("offset", String(params.offset))
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/orders/dispatch-prep/planning-rows?${qs}`,
    { headers: getAuthHeaders(), signal: params.signal },
    DISPATCH_PREP_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar pre-planificación")
  }
  return res.json() as Promise<DistribuidoraDispatchPrepPlanningRowsResponse>
}

export type DistribuidoraTruck = {
  id: number
  name: string
  plate: string
  max_weight_kg: number
  km_per_liter?: number
  fuel_type?: string
}

export function distribuidoraTruckCapacityLabel(
  t: Pick<DistribuidoraTruck, "name" | "max_weight_kg">,
): string {
  return `${t.name} (${t.max_weight_kg} kg)`
}

export async function getDistribuidoraTrucks(params?: {
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraTruck[] }> {
  const res = await fetch(`${API_URL}/distribuidora/trucks`, {
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar camiones")
  }
  return res.json() as Promise<{ items: DistribuidoraTruck[] }>
}

/** POST /distribuidora/sync-orders (200 síncrono) | /distribuidora/sync-sales (202 ``queued`` o error). */
export type DistribuidoraTypedSyncResponse = {
  ok: boolean
  status?: string
  stats?: Record<string, unknown>
  error?: string
  /** Solo sync-orders (respuesta 200). */
  orders_processed?: number
  /** Filas insertadas en ``document_related`` tras sync-orders. */
  related_processed?: number
  message?: string
}

function _detailFromBody(data: unknown): string | undefined {
  if (!data || typeof data !== "object") return undefined
  const d = (data as { detail?: unknown }).detail
  if (typeof d === "string") return d
  if (Array.isArray(d) && d.length && typeof d[0] === "object" && d[0] !== null) {
    const msg = (d[0] as { msg?: unknown }).msg
    if (typeof msg === "string") return msg
  }
  return undefined
}

export async function postDistribuidoraSyncOrders(params?: {
  signal?: AbortSignal
}): Promise<DistribuidoraTypedSyncResponse> {
  const res = await fetch(`${API_URL}/distribuidora/sync-orders`, {
    method: "POST",
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  const data = (await res.json().catch(() => ({}))) as Record<string, unknown>
  if (res.status === 409) {
    return {
      ok: false,
      error: _detailFromBody(data) ?? "sync_en_curso",
    }
  }
  if (!res.ok) {
    return {
      ok: false,
      error: _detailFromBody(data) ?? `HTTP ${res.status}`,
    }
  }
  const st = typeof data.status === "string" ? data.status : undefined
  const orders_processed =
    typeof data.orders_processed === "number" ? data.orders_processed : undefined
  const related_processed =
    typeof data.related_processed === "number" ? data.related_processed : undefined
  const message = typeof data.message === "string" ? data.message : undefined
  return {
    ok: true,
    status:
      st ??
      (res.status === 200
        ? "complete"
        : res.status === 202
          ? "queued"
          : undefined),
    orders_processed,
    related_processed,
    message,
    stats: data.stats as Record<string, unknown> | undefined,
  }
}

/** GET /distribuidora/sync-status — cursores ``sync_process_cursor`` + último ``sync_logs`` por proceso. */
export type DistribuidoraSyncStatusBranch = {
  last_run: string | null
  processed: number
  visibles?: number
  ocultas?: number
  boletas?: number
  facturas?: number
  nc?: number
  monto_neto?: number
  status: "ok" | "running" | "error"
}

export type DistribuidoraLiveSyncLayerStatus = {
  label: string
  last_success_at: string | null
  status: string
  items_processed?: number
  error_summary?: string | null
  last_window_from?: string | null
  last_window_to?: string | null
}

export type DistribuidoraSyncStatusResponse = {
  orders: DistribuidoraSyncStatusBranch
  sales: DistribuidoraSyncStatusBranch
  sync_lock_active: boolean
  live_sync?: {
    documents_live?: DistribuidoraLiveSyncLayerStatus
    details_live?: DistribuidoraLiveSyncLayerStatus
    related_live?: DistribuidoraLiveSyncLayerStatus
    probable_live?: DistribuidoraLiveSyncLayerStatus
  }
  live_sync_global_busy?: boolean
}

export type DistribuidoraLiveSyncNowResponse = {
  ok: boolean
  status: string
  message?: string
  started_at?: string
  finished_at?: string
  duration_seconds?: number
  documents?: Record<string, unknown>
  details?: Record<string, unknown>
  related?: Record<string, unknown>
  probable_matches?: Record<string, unknown>
}

export async function getDistribuidoraSyncStatus(params?: {
  signal?: AbortSignal
}): Promise<DistribuidoraSyncStatusResponse> {
  const res = await fetch(`${API_URL}/distribuidora/sync-status`, {
    method: "GET",
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  if (!res.ok) {
    const t = await res.text().catch(() => "")
    throw new Error(t || `HTTP ${res.status}`)
  }
  return res.json() as Promise<DistribuidoraSyncStatusResponse>
}

/** POST /distribuidora/sync/live-now — cadena sync live on-demand. */
export async function postDistribuidoraSyncLiveNow(params?: {
  signal?: AbortSignal
}): Promise<DistribuidoraLiveSyncNowResponse> {
  const res = await fetch(`${API_URL}/distribuidora/sync/live-now`, {
    method: "POST",
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  const data = (await res.json().catch(() => ({}))) as DistribuidoraLiveSyncNowResponse
  if (res.status === 409) {
    return {
      ok: false,
      status: "already_running",
      message:
        data.message ?? "Ya hay una sincronización en ejecución",
    }
  }
  if (!res.ok) {
    return {
      ok: false,
      status: "error",
      message:
        typeof data === "object" && data && "detail" in data
          ? String((data as { detail?: unknown }).detail)
          : `HTTP ${res.status}`,
    }
  }
  return { ...data, ok: data.ok !== false }
}

export async function postDistribuidoraSyncSales(params?: {
  signal?: AbortSignal
}): Promise<DistribuidoraTypedSyncResponse> {
  const res = await fetch(`${API_URL}/distribuidora/sync-sales`, {
    method: "POST",
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  const data = (await res.json().catch(() => ({}))) as Record<string, unknown>
  if (res.status === 409) {
    return { ok: false, error: _detailFromBody(data) ?? "sync_en_curso" }
  }
  if (!res.ok) {
    return {
      ok: false,
      error: _detailFromBody(data) ?? `HTTP ${res.status}`,
    }
  }
  const st = typeof data.status === "string" ? data.status : undefined
  const orders_processed =
    typeof data.orders_processed === "number" ? data.orders_processed : undefined
  const related_processed =
    typeof data.related_processed === "number" ? data.related_processed : undefined
  const message = typeof data.message === "string" ? data.message : undefined
  return {
    ok: true,
    status:
      st ??
      (res.status === 200
        ? "complete"
        : res.status === 202
          ? "queued"
          : undefined),
    orders_processed,
    related_processed,
    message,
    stats: data.stats as Record<string, unknown> | undefined,
  }
}

function sleepMs(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => resolve(), ms)
    if (!signal) return
    const onAbort = () => {
      clearTimeout(t)
      reject(new DOMException("Aborted", "AbortError"))
    }
    if (signal.aborted) {
      clearTimeout(t)
      onAbort()
      return
    }
    signal.addEventListener("abort", onAbort, { once: true })
  })
}

/**
 * Tras un POST sync en background, espera a que ``sync-status`` refleje un nuevo ``last_run``
 * o estado error (polling).
 */
export async function waitDistribuidoraTypedSyncComplete(opts: {
  branch: "orders" | "sales"
  baselineLastRun: string | null
  timeoutMs?: number
  pollMs?: number
  signal?: AbortSignal
}): Promise<DistribuidoraSyncStatusResponse> {
  const { branch, baselineLastRun, timeoutMs = 180_000, pollMs = 2500, signal } = opts
  const deadline = Date.now() + timeoutMs
  const label = branch === "orders" ? "órdenes" : "ventas"
  while (Date.now() < deadline) {
    const s = await getDistribuidoraSyncStatus({ signal })
    const b = branch === "orders" ? s.orders : s.sales
    if (b.status === "error") {
      throw new Error(`El sync de ${label} terminó con error.`)
    }
    if (b.status === "running") {
      await sleepMs(pollMs, signal)
      continue
    }
    if ((b.last_run ?? null) !== (baselineLastRun ?? null)) {
      return s
    }
    await sleepMs(pollMs, signal)
  }
  throw new Error(
    `Tiempo de espera (${Math.round(timeoutMs / 1000)} s) esperando el sync de ${label}.`,
  )
}

/** Respuesta inmediata al encolar resync OC (el trabajo corre en background). */
export type DistribuidoraResyncOcStartResponse = {
  ok: boolean
  job_id?: string
  status?: string
  emission_date_from?: string
  emission_date_to?: string
  error?: string
}

export type DistribuidoraResyncOcJobStatusResponse = {
  ok: boolean
  job_id?: string
  status?: string
  processed_count?: number
  updated_count?: number
  error_count?: number
  message?: string
  emission_date_from?: string
  emission_date_to?: string
  started_at?: string | null
  finished_at?: string | null
  error?: string
}

/** POST /distribuidora/resync-oc — encola job; usar polling con ``getDistribuidoraResyncOcStatus``. */
export async function postDistribuidoraResyncOc(params?: {
  emission_date_from?: string
  emission_date_to?: string
  signal?: AbortSignal
}): Promise<DistribuidoraResyncOcStartResponse> {
  const body =
    params?.emission_date_from && params?.emission_date_to
      ? JSON.stringify({
          emission_date_from: params.emission_date_from,
          emission_date_to: params.emission_date_to,
        })
      : "{}"
  const res = await fetch(`${API_URL}/distribuidora/resync-oc`, {
    method: "POST",
    headers: {
      ...getAuthHeaders(),
      "Content-Type": "application/json",
    },
    body,
    signal: params?.signal,
  })
  const data = (await res.json().catch(() => ({}))) as DistribuidoraResyncOcStartResponse
  if (!res.ok) {
    return { ok: false, error: data?.error ?? `HTTP ${res.status}` }
  }
  return {
    ok: Boolean(data.ok),
    job_id: typeof data.job_id === "string" ? data.job_id : undefined,
    status: typeof data.status === "string" ? data.status : undefined,
    emission_date_from:
      typeof data.emission_date_from === "string" ? data.emission_date_from : undefined,
    emission_date_to:
      typeof data.emission_date_to === "string" ? data.emission_date_to : undefined,
    error: data.error,
  }
}

/** GET /distribuidora/resync-oc/status/{job_id} */
export async function getDistribuidoraResyncOcStatus(
  jobId: string,
  params?: { signal?: AbortSignal },
): Promise<DistribuidoraResyncOcJobStatusResponse> {
  const res = await fetch(
    `${API_URL}/distribuidora/resync-oc/status/${encodeURIComponent(jobId)}`,
    {
      method: "GET",
      headers: getAuthHeaders(),
      signal: params?.signal,
    },
  )
  const data = (await res.json().catch(() => ({}))) as DistribuidoraResyncOcJobStatusResponse
  if (!res.ok) {
    return { ok: false, error: data?.error ?? `HTTP ${res.status}` }
  }
  return {
    ok: Boolean(data.ok),
    job_id: typeof data.job_id === "string" ? data.job_id : undefined,
    status: typeof data.status === "string" ? data.status : undefined,
    processed_count:
      typeof data.processed_count === "number" ? data.processed_count : undefined,
    updated_count: typeof data.updated_count === "number" ? data.updated_count : undefined,
    error_count: typeof data.error_count === "number" ? data.error_count : undefined,
    message: typeof data.message === "string" ? data.message : undefined,
    emission_date_from:
      typeof data.emission_date_from === "string" ? data.emission_date_from : undefined,
    emission_date_to:
      typeof data.emission_date_to === "string" ? data.emission_date_to : undefined,
    started_at: data.started_at ?? null,
    finished_at: data.finished_at ?? null,
    error: data.error,
  }
}

const RESYNC_OC_POLL_MS = 2000

/** Poll hasta ``status`` done o error (o hasta ``AbortSignal``). */
export async function pollDistribuidoraResyncOcJobUntilTerminal(
  jobId: string,
  options: {
    signal?: AbortSignal
    onStatus?: (s: DistribuidoraResyncOcJobStatusResponse) => void
    intervalMs?: number
  } = {},
): Promise<DistribuidoraResyncOcJobStatusResponse> {
  const intervalMs = options.intervalMs ?? RESYNC_OC_POLL_MS
  for (;;) {
    if (options.signal?.aborted) {
      throw new DOMException("Aborted", "AbortError")
    }
    const st = await getDistribuidoraResyncOcStatus(jobId, { signal: options.signal })
    options.onStatus?.(st)
    if (!st.ok) return st
    const terminal = st.status === "done" || st.status === "error"
    if (terminal) return st
    await new Promise((r) => setTimeout(r, intervalMs))
  }
}

/** Fila de GET /distribuidora/planificacion/orders (OC tipo 33 + observaciones). */
export type DistribuidoraPlanificacionOrderRow = {
  document_id: number
  client_id?: number | null
  oc?: number | null
  nombre_fantasia?: string | null
  municipality?: string | null
  direccion?: string | null
  comuna?: string | null
  seller_name?: string | null
  total_amount?: number | null
  has_georef?: boolean | null
  lat?: number | null
  lng?: number | null
  observations?: string | null
  estado_real?: string | null
  purchase_status?: string | null
  associated_document_label?: string | null
  display_score?: number | null
  probable_score?: number | null
  probable_tier?: string | null
}

export async function getDistribuidoraPlanificacionOrders(params: {
  emission_date_from: string
  emission_date_to: string
  delivery_day?: string
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraPlanificacionOrderRow[] }> {
  const qs = new URLSearchParams()
  qs.set("emission_date_from", params.emission_date_from)
  qs.set("emission_date_to", params.emission_date_to)
  if (params.delivery_day?.trim()) qs.set("delivery_day", params.delivery_day.trim())
  const res = await fetch(`${API_URL}/distribuidora/planificacion/orders?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar órdenes para planificación")
  }
  return res.json() as Promise<{ items: DistribuidoraPlanificacionOrderRow[] }>
}

export type OrsStopOrdered = {
  document_id: number
  stop_index: number
  lat: number
  lng: number
}

export type OrsRouteCostBreakdown = {
  fuel_clp: number
  ferry_clp: number
  toll_clp: number
  driver_clp: number
  crew_clp?: number
  bonus_clp?: number
  per_diem_clp?: number
  lodging_clp?: number
  total_clp: number
}

export type DistribuidoraPlanificacionOrsRoute = {
  camion: string
  truck_id?: number | null
  truck_name?: string | null
  distance_km: number
  duration_min: number
  geometry: { type: string; coordinates: number[][] }
  coordinates: number[][]
  stops_ordered?: OrsStopOrdered[]
  liters_estimated?: number
  fuel_cost_clp?: number
  km_per_liter_used?: number
  fuel_type?: string
  driver_count?: number
  assistant_count?: number
  driver_cost_clp?: number
  assistant_cost_clp?: number
  crew_cost_clp?: number
  cost_breakdown?: OrsRouteCostBreakdown
  includes_depot_return?: boolean
}

export type DistribuidoraPlanificacionOrsTotals = {
  distance_km: number
  duration_min: number
  liters_estimated: number
  fuel_cost_clp: number
  crew_cost_clp?: number
  total_cost_clp?: number
}

export type DistribuidoraPlanificacionCrewDefaults = {
  driver_cost_clp_per_trip: number
  assistant_cost_clp_per_trip: number
  bonus_clp_per_route?: number
  per_diem_clp_per_day?: number
  lodging_clp_per_night?: number
  enabled_modules?: string[]
}

export type DistribuidoraPlanificacionOrsResponse = {
  routes: DistribuidoraPlanificacionOrsRoute[]
  depot: { lat: number; lng: number }
  diesel_price_per_liter: number
  crew_defaults?: DistribuidoraPlanificacionCrewDefaults
  totals: DistribuidoraPlanificacionOrsTotals
}

export async function getDistribuidoraPlanificacionFuelConfig(params?: {
  signal?: AbortSignal
}): Promise<{ diesel_price_per_liter: number; depot: { lat: number; lng: number } }> {
  const res = await fetch(`${API_URL}/distribuidora/planificacion/fuel-config`, {
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar configuración combustible")
  }
  return res.json() as Promise<{
    diesel_price_per_liter: number
    depot: { lat: number; lng: number }
  }>
}

export async function putDistribuidoraPlanificacionFuelConfig(
  diesel_price_per_liter: number,
  params?: { signal?: AbortSignal },
): Promise<{ diesel_price_per_liter: number; depot: { lat: number; lng: number } }> {
  const res = await fetch(`${API_URL}/distribuidora/planificacion/fuel-config`, {
    method: "PUT",
    headers: getAuthHeaders(),
    body: JSON.stringify({ diesel_price_per_liter }),
    signal: params?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al guardar configuración combustible")
  }
  return res.json() as Promise<{
    diesel_price_per_liter: number
    depot: { lat: number; lng: number }
  }>
}

export async function getDistribuidoraPlanificacionCrewConfig(params?: {
  signal?: AbortSignal
}): Promise<DistribuidoraPlanificacionCrewDefaults> {
  const res = await fetch(`${API_URL}/distribuidora/planificacion/crew-config`, {
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar tarifas de personal")
  }
  return res.json() as Promise<DistribuidoraPlanificacionCrewDefaults>
}

export type PlanificacionRouteCrewRow = {
  camion: string
  truck_id?: number | null
  driver_count: number
  assistant_count: number
  driver_cost_clp?: number
  assistant_cost_clp?: number
}

export async function getDistribuidoraPlanificacionRouteCrew(params: {
  planSessionId: string
  signal?: AbortSignal
}): Promise<{
  plan_session_id: string
  routes: PlanificacionRouteCrewRow[]
  defaults: DistribuidoraPlanificacionCrewDefaults
}> {
  const qs = new URLSearchParams({ plan_session_id: params.planSessionId })
  const res = await fetch(
    `${API_URL}/distribuidora/planificacion/route-crew?${qs}`,
    { headers: getAuthHeaders(), signal: params.signal },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar dotación de rutas")
  }
  return res.json() as Promise<{
    plan_session_id: string
    routes: PlanificacionRouteCrewRow[]
    defaults: DistribuidoraPlanificacionCrewDefaults
  }>
}

export async function putDistribuidoraPlanificacionRouteCrew(params: {
  planSessionId: string
  routes: PlanificacionRouteCrewRow[]
  signal?: AbortSignal
}): Promise<{
  plan_session_id: string
  routes: PlanificacionRouteCrewRow[]
  defaults: DistribuidoraPlanificacionCrewDefaults
}> {
  const res = await fetch(`${API_URL}/distribuidora/planificacion/route-crew`, {
    method: "PUT",
    headers: getAuthHeaders(),
    body: JSON.stringify({
      plan_session_id: params.planSessionId,
      routes: params.routes,
    }),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al guardar dotación de rutas")
  }
  return res.json() as Promise<{
    plan_session_id: string
    routes: PlanificacionRouteCrewRow[]
    defaults: DistribuidoraPlanificacionCrewDefaults
  }>
}

export type PlanificacionOperationalCostsRow = {
  plan_session_id: string
  truck_id: number
  ferry_clp: number
  per_diem_clp: number
  other_clp: number
  diesel_clp_per_liter?: number | null
}

export async function getDistribuidoraPlanificacionOperationalCosts(params: {
  planSessionId: string
  truckId: number
  signal?: AbortSignal
}): Promise<PlanificacionOperationalCostsRow> {
  const qs = new URLSearchParams({
    plan_session_id: params.planSessionId,
    truck_id: String(params.truckId),
  })
  const res = await fetch(
    `${API_URL}/distribuidora/planificacion/operational-costs?${qs}`,
    { headers: getAuthHeaders(), signal: params.signal },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar costos operacionales")
  }
  return res.json() as Promise<PlanificacionOperationalCostsRow>
}

export async function putDistribuidoraPlanificacionOperationalCosts(body: {
  plan_session_id: string
  truck_id: number
  ferry_clp: number
  per_diem_clp: number
  other_clp: number
  diesel_clp_per_liter?: number | null
  signal?: AbortSignal
}): Promise<PlanificacionOperationalCostsRow> {
  const res = await fetch(`${API_URL}/distribuidora/planificacion/operational-costs`, {
    method: "PUT",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
    signal: body.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al guardar costos operacionales")
  }
  return res.json() as Promise<PlanificacionOperationalCostsRow>
}

export async function postDistribuidoraPlanificacionOrsRoutes(params: {
  planSessionId?: string | null
  dieselPricePerLiter?: number | null
  routes: {
    camion: string
    truck_id?: number | null
    driver_count?: number
    assistant_count?: number
    stops: { document_id: number; lat: number; lng: number }[]
  }[]
  signal?: AbortSignal
}): Promise<DistribuidoraPlanificacionOrsResponse> {
  const body: Record<string, unknown> = { routes: params.routes }
  if (params.planSessionId?.trim()) {
    body.plan_session_id = params.planSessionId.trim()
  }
  if (params.dieselPricePerLiter != null && params.dieselPricePerLiter > 0) {
    body.diesel_price_per_liter = params.dieselPricePerLiter
  }
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/planificacion/ors-routes`,
    {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(body),
      signal: params.signal,
    },
    ORS_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al calcular rutas ORS")
  }
  return res.json() as Promise<DistribuidoraPlanificacionOrsResponse>
}

export type DispatchPlanStatus =
  | "draft"
  | "planned"
  | "invoicing"
  | "ready_for_picking"
  | "picking_generated"
  | "closed"
  | "dispatched"
  | "delivered"
  | "squared"

export type DispatchPlanSummary = {
  id: number
  plan_session_id?: string | null
  planning_code?: string | null
  planning_name?: string | null
  planning_date: string
  truck_id?: number | null
  truck_name?: string | null
  route_name: string
  status: DispatchPlanStatus
  driver_count?: number
  assistant_count?: number
  km_total?: number
  total_route_cost_clp?: number
  final_margin_clp?: number | null
  net_operational_clp?: number | null
  order_count?: number
  total_oc_amount?: number
  invoiced_confirmed?: number
  invoiced_probable?: number
  invoiced_pending?: number
  confirmed_at?: string | null
  created_at?: string
}

export async function listDispatchPlansRecent(params?: {
  limit?: number
  signal?: AbortSignal
}): Promise<{ items: DispatchPlanSummary[] }> {
  const qs = new URLSearchParams()
  qs.set("limit", String(params?.limit ?? 50))
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans?${qs}`,
    { headers: getAuthHeaders(), signal: params?.signal },
    DEFAULT_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al listar planificaciones")
  }
  return res.json() as Promise<{ items: DispatchPlanSummary[] }>
}

export async function getDispatchPlanHeader(
  planId: number,
  signal?: AbortSignal,
): Promise<{ plan: DispatchPlanSummary }> {
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/header`,
    { headers: getAuthHeaders(), signal },
    DEFAULT_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar cabecera del plan")
  }
  return res.json() as Promise<{ plan: DispatchPlanSummary }>
}

export type DispatchPlanLoadSummary = {
  header: {
    planning_code: string
    planning_date: string
    planning_name: string
    truck_name: string
    driver_name?: string
    driver_label?: string
    assistant_label?: string
    assistant_names?: string[]
    route_name: string
    communes?: string
    sello?: string
  }
  kpis: {
    clients: number
    documents: number
    /** Venta OC total del plan (todas las órdenes). */
    oc_total_amount_clp?: number
    /** Venta facturada/confirmada (related + auto). */
    confirmed_sales_clp?: number
    /** Venta en picking generado (documentos incluidos). */
    picking_sales_clp?: number
    /** Compat: prioriza picking → confirmada → OC. */
    sales_total_clp: number
    distinct_products: number
    total_units: number
    estimated_boxes: number
  }
  invoicing: {
    confirmed_manual: number
    confirmed_auto: number
    confirmed_total: number
    probable: number
    pending: number
    oc_total_amount_clp?: number
    confirmed_amount_clp: number
    probable_amount_clp: number
    pending_amount_clp: number
  }
  costs: {
    fuel_clp: number
    crew_clp: number
    tolls_clp: number
    ferry_clp: number
    extras_clp: number
    route_total_clp: number
  }
  results: {
    commercial_margin_clp?: number | null
    net_operational_clp?: number | null
    margin_visible: boolean
    margin_message?: string | null
  }
  picking: {
    has_snapshot: boolean
    version?: number | null
    picking_id?: number | null
    generated_at?: string | null
    ready_to_generate: boolean
    ready_reason?: string | null
  }
  operational_status: string
  operational_status_label: string
}

export type DispatchPlanDashboard = {
  plan: DispatchPlanSummary
  load_summary?: DispatchPlanLoadSummary
  invoicing: {
    total_orders: number
    total_oc_amount_clp: number
    confirmed: {
      count: number
      amount_clp: number
      auto_confirmed_count?: number
    }
    probable: { count: number; amount_clp: number }
    pending: { count: number; amount_clp: number }
  }
  invoiced_items: DispatchPlanInvoicedRow[]
  warnings: { oc_document_id: number; message: string }[]
  probable_notes: { oc_document_id: number; message: string }[]
  margin: {
    visible: boolean
    restricted?: boolean
    unavailable?: boolean
    partial?: boolean
    message?: string
    commercial_margin_clp?: number | null
    invoiced_revenue_clp?: number
    invoiced_cost_clp?: number | null
    route_cost_clp?: number
    net_operational_clp?: number | null
    source?: string
  } | null
  picking: {
    client_endpoint: string
    product_endpoint: string
    ready: boolean
    reason?: string | null
  }
  invoicing_source?: string
  /** true si el backend degradó la respuesta por error interno */
  degraded?: boolean
}

/** Dashboard liviano (sin pickings; margen opcional). */
export const DASHBOARD_FETCH_TIMEOUT_MS = 120_000

export async function getDispatchPlanDashboard(
  planId: number,
  signal?: AbortSignal,
  opts?: { include_margin?: boolean },
): Promise<DispatchPlanDashboard> {
  const qs = new URLSearchParams()
  if (opts?.include_margin) qs.set("include_margin", "true")
  const q = qs.toString()
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/dashboard${q ? `?${q}` : ""}`,
    { headers: getAuthHeaders(), signal },
    DASHBOARD_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar dashboard del plan")
  }
  return res.json() as Promise<DispatchPlanDashboard>
}

export async function repairDispatchPlanSnapshot(planId: number): Promise<unknown> {
  const res = await fetch(`${API_URL}/distribuidora/dispatch-plans/${planId}/repair-snapshot`, {
    method: "POST",
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al reparar snapshot")
  }
  return res.json()
}

export type DispatchPlanInvoicedRow = {
  oc_document_id: number
  oc_number?: number | null
  route_order?: number
  status: "confirmed" | "probable" | "missing"
  relation_source?: string | null
  is_invoiced_confirmed?: boolean | null
  is_auto_confirmed?: boolean | null
  related_document_id?: number | null
  related_document_number?: number | null
  related_document_type_label?: string | null
  probable_document_number?: number | null
  probable_score?: number | null
}

export async function getDispatchPlansBySession(
  planSessionId: string,
  signal?: AbortSignal,
): Promise<{
  items: DispatchPlanSummary[]
}> {
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/by-session/${encodeURIComponent(planSessionId)}`,
    { headers: getAuthHeaders(), signal },
    DEFAULT_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar planes de despacho")
  }
  return res.json() as Promise<{ items: DispatchPlanSummary[] }>
}

export async function confirmDispatchPlan(body: {
  plan_session_id: string
  truck_id: number
  route_name: string
  planning_name?: string | null
  driver_count: number
  assistant_count: number
  driver_cost_clp: number
  assistant_cost_clp: number
  diesel_price_per_liter: number
  km_total: number
  duration_min: number
  liters_estimated: number
  fuel_cost_clp: number
  ferry_cost_clp: number
  toll_cost_clp: number
  extras_cost_clp: number
  crew_cost_clp: number
  total_route_cost_clp: number
  route_geometry?: Record<string, unknown> | null
  orders: {
    oc_document_id: number
    oc_number?: number | null
    route_order: number
    client_id?: number | null
    client_name?: string | null
    address?: string | null
    city?: string | null
    seller_name?: string | null
    oc_total_amount?: number | null
    lat?: number | null
    lng?: number | null
  }[]
}): Promise<{ plan: DispatchPlanSummary; orders: unknown[] }> {
  const res = await fetch(`${API_URL}/distribuidora/dispatch-plans/confirm`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al confirmar planificación")
  }
  return res.json() as Promise<{ plan: DispatchPlanSummary; orders: unknown[] }>
}

export async function getDispatchPlanInvoicedDocuments(planId: number): Promise<{
  dispatch_plan_id: number
  items: DispatchPlanInvoicedRow[]
  summary: {
    confirmed: number
    auto_confirmed?: number
    probable: number
    missing: number
    total: number
  }
  warnings: { oc_document_id: number; oc_number?: number | null; message: string }[]
  probable_notes: {
    oc_document_id: number
    message: string
    probable_document_number?: number | null
    probable_score?: number | null
  }[]
  ready_for_picking: boolean
}> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/invoiced-documents`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al revisar facturación")
  }
  return res.json() as Promise<{
    dispatch_plan_id: number
    items: DispatchPlanInvoicedRow[]
    summary: {
      confirmed: number
      auto_confirmed?: number
      probable: number
      missing: number
      total: number
    }
    warnings: { oc_document_id: number; oc_number?: number | null; message: string }[]
    probable_notes: {
      oc_document_id: number
      message: string
      probable_document_number?: number | null
      probable_score?: number | null
    }[]
    ready_for_picking: boolean
  }>
}

export async function downloadDispatchPlanBillingExcel(planId: number): Promise<void> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/billing-export`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al descargar Excel de facturación")
  }
  const blob = await res.blob()
  const cd = res.headers.get("Content-Disposition")
  const m = cd?.match(/filename="([^"]+)"/)
  const name = m?.[1] ?? `facturacion_plan_${planId}.xlsx`
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export type DispatchPlanPickingHeader = {
  plan_id: number
  planning_number: string
  planning_name?: string
  delivery_date: string
  route_name: string
  communes: string
  truck_name: string
  driver_name?: string
  driver_label: string
  assistant_label: string
  assistant_names?: string[]
  sello: string
  load_kpis?: {
    clients: number
    documents: number
    sales_total_clp: number
    distinct_products: number
    total_units: number
    estimated_boxes: number
  }
  load_batch?: {
    id: number
    name: string
    description?: string | null
  }
}

export type DispatchPlanPickingClientRow = {
  route_order?: number | null
  client_id?: number | null
  city?: string
  client_name?: string
  fantasy_name?: string
  address?: string
  phone?: string
  document_number?: number | null
  document_type?: string
  payment_method?: string
  seller_name?: string
  seller_phone?: string
  observations?: string
  delivery_notes?: string
  document_total?: number | null
  related_document_id?: number
  relation_source?: string | null
  inclusion?: string
  is_probable_included?: boolean
  probable_score?: number | null
}

export type DispatchPlanPickingProductRow = {
  sucursal_bodega?: string
  unidades?: number | null
  tipo_producto?: string
  producto?: string
  variante?: string
  product_name?: string
  variant_name?: string
  display_name?: string
  producto_variante?: string
  codigo_barras?: string | null
  cajas?: number | null
  cajas_efectivas?: number | null
  units_per_box?: number | null
  units_per_box_efectivo?: number | null
  sin_unidad_caja?: boolean
  total_monto?: number | null
}

export type DispatchPlanPickingClientResponse = {
  dispatch_plan_id: number
  picking_id?: number
  version?: number
  is_current?: boolean
  generated_at?: string
  source?: string
  ready?: boolean
  reason?: string | null
  header?: DispatchPlanPickingHeader
  clients: DispatchPlanPickingClientRow[]
  warnings?: string[]
  include_probable?: boolean
  totals?: { stops: number; document_total_clp: number }
  degraded?: boolean
}

export type DispatchPlanPickingProductResponse = {
  dispatch_plan_id: number
  picking_id?: number
  version?: number
  is_current?: boolean
  generated_at?: string
  source?: string
  ready?: boolean
  reason?: string | null
  header?: DispatchPlanPickingHeader
  items: DispatchPlanPickingProductRow[]
  warnings?: string[]
  include_probable?: boolean
  totals?: {
    lines: number
    unidades: number
    cajas: number
    total_monto_clp: number
  }
  degraded?: boolean
}

export const DISPATCH_PLAN_PICKING_WAIT_MESSAGE =
  "Los pickings estarán disponibles una vez existan documentos facturados o relacionados."

export const DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE =
  "No hay documentos facturados confirmados para este plan."

export type DispatchPlanPickingGenerateResponse = DispatchPlanPickingClientResponse & {
  items?: DispatchPlanPickingProductRow[]
}

export async function generateDispatchPlanPicking(
  planId: number,
  opts?: { validate?: boolean; includeProbable?: boolean; reason?: string },
): Promise<DispatchPlanPickingGenerateResponse> {
  const qs = new URLSearchParams()
  qs.set("validate", opts?.validate === false ? "false" : "true")
  if (opts?.includeProbable) qs.set("include_probable", "true")
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking/generate?${qs}`,
    {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(opts?.reason ? { reason: opts.reason } : {}),
    },
    DASHBOARD_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al generar picking")
  }
  return res.json() as Promise<DispatchPlanPickingGenerateResponse>
}

export async function listDispatchPlanPickings(
  planId: number,
): Promise<{
  dispatch_plan_id: number
  items: {
    picking_id: number
    version: number
    is_current?: boolean
    generated_at?: string
  }[]
}> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/pickings`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al listar versiones de picking")
  }
  return res.json() as Promise<{
    dispatch_plan_id: number
    items: { picking_id: number; version: number; is_current?: boolean }[]
  }>
}

async function downloadDispatchPlanPickingExport(
  planId: number,
  kind: "cliente" | "producto",
  opts?: { version?: number; pickingId?: number },
): Promise<void> {
  const qs = new URLSearchParams()
  if (opts?.version != null) qs.set("version", String(opts.version))
  if (opts?.pickingId != null) qs.set("picking_id", String(opts.pickingId))
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-${kind}/export?${qs}`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al descargar Excel de picking")
  }
  const blob = await res.blob()
  const cd = res.headers.get("Content-Disposition")
  const m = cd?.match(/filename="([^"]+)"/)
  const name = m?.[1] ?? `picking_${kind}_${planId}.xlsx`
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export async function downloadDispatchPlanPickingClienteExcel(
  planId: number,
  opts?: { version?: number; pickingId?: number },
): Promise<void> {
  return downloadDispatchPlanPickingExport(planId, "cliente", opts)
}

export async function downloadDispatchPlanPickingProductoExcel(
  planId: number,
  opts?: { version?: number; pickingId?: number },
): Promise<void> {
  return downloadDispatchPlanPickingExport(planId, "producto", opts)
}

export async function getDispatchPlanPickingCliente(
  planId: number,
  opts?: {
    validate?: boolean
    includeProbable?: boolean
    version?: number
    pickingId?: number
    loadBatchId?: number
    signal?: AbortSignal
  },
): Promise<DispatchPlanPickingClientResponse> {
  const qs = new URLSearchParams()
  if (opts?.validate === true) qs.set("validate", "true")
  if (opts?.includeProbable) qs.set("include_probable", "true")
  if (opts?.version != null) qs.set("version", String(opts.version))
  if (opts?.pickingId != null) qs.set("picking_id", String(opts.pickingId))
  if (opts?.loadBatchId != null) qs.set("load_batch_id", String(opts.loadBatchId))
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-cliente?${qs}`,
    { headers: getAuthHeaders(), signal: opts?.signal },
    DASHBOARD_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar picking por cliente")
  }
  const data = (await res.json()) as DispatchPlanPickingClientResponse
  if (data.ready === false && data.reason) {
    return data
  }
  return data
}

export async function getDispatchPlanPickingProducto(
  planId: number,
  opts?: {
    validate?: boolean
    includeProbable?: boolean
    version?: number
    pickingId?: number
    loadBatchId?: number
    signal?: AbortSignal
  },
): Promise<DispatchPlanPickingProductResponse> {
  const qs = new URLSearchParams()
  if (opts?.validate === true) qs.set("validate", "true")
  if (opts?.includeProbable) qs.set("include_probable", "true")
  if (opts?.version != null) qs.set("version", String(opts.version))
  if (opts?.pickingId != null) qs.set("picking_id", String(opts.pickingId))
  if (opts?.loadBatchId != null) qs.set("load_batch_id", String(opts.loadBatchId))
  const res = await fetchWithTimeout(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-producto?${qs}`,
    { headers: getAuthHeaders(), signal: opts?.signal },
    DASHBOARD_FETCH_TIMEOUT_MS,
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar picking por producto")
  }
  const data = (await res.json()) as DispatchPlanPickingProductResponse
  if (data.ready === false && data.reason) {
    return data
  }
  return data
}

/** @deprecated Usar getDispatchPlanPickingCliente */
export async function getDispatchPlanPickingByClient(planId: number): Promise<{
  dispatch_plan_id: number
  clients: unknown[]
}> {
  return getDispatchPlanPickingCliente(planId)
}

/** @deprecated Usar getDispatchPlanPickingProducto */
export async function getDispatchPlanPickingByProduct(planId: number): Promise<{
  dispatch_plan_id: number
  items: unknown[]
}> {
  return getDispatchPlanPickingProducto(planId)
}

export type DispatchPlanLoadBatch = {
  id: number
  dispatch_plan_id: number
  sort_order: number
  name: string
  description?: string | null
  created_at?: string
  updated_at?: string
}

export type DispatchPlanDocumentAssignment = {
  id: number
  dispatch_plan_id: number
  load_batch_id: number | null
  related_document_id: number
  oc_document_id?: number | null
  document_number?: number | null
  client_name?: string | null
  document_total?: number | null
  assigned_at?: string
  load_batch_name?: string | null
  load_batch_sort_order?: number | null
}

export type DispatchPlanPickingAssignmentsResponse = {
  dispatch_plan_id: number
  batches: DispatchPlanLoadBatch[]
  assignments: DispatchPlanDocumentAssignment[]
  has_picking: boolean
  picking_version?: number | null
  picking_id?: number | null
}

export type DispatchPlanOrderSearchHit = {
  oc_document_id: number
  oc_number?: number | null
  client_id?: number | null
  client_name?: string | null
  oc_total_amount?: number | null
}

export type DispatchPlanOrderEvent = {
  id: number
  dispatch_plan_id: number
  action: string
  user_name?: string | null
  reason?: string | null
  oc_document_id?: number | null
  oc_number?: number | null
  picking_id?: number | null
  picking_version?: number | null
  created_at?: string
}

export async function getDispatchPlanLoadBatches(
  planId: number,
): Promise<{ dispatch_plan_id: number; items: DispatchPlanLoadBatch[] }> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/load-batches`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al cargar pickings"))
  return res.json()
}

export async function createDispatchPlanLoadBatch(
  planId: number,
  body: { name: string; description?: string },
): Promise<DispatchPlanLoadBatch> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/load-batches`,
    {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(body),
    },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al crear picking"))
  return res.json()
}

export async function updateDispatchPlanLoadBatch(
  planId: number,
  batchId: number,
  body: { name: string; description?: string; sort_order?: number },
): Promise<DispatchPlanLoadBatch> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/load-batches/${batchId}`,
    {
      method: "PUT",
      headers: getAuthHeaders(),
      body: JSON.stringify(body),
    },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al actualizar picking"))
  return res.json()
}

export async function deleteDispatchPlanLoadBatch(
  planId: number,
  batchId: number,
): Promise<void> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/load-batches/${batchId}`,
    { method: "DELETE", headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al eliminar picking"))
}

export async function getDispatchPlanPickingAssignments(
  planId: number,
): Promise<DispatchPlanPickingAssignmentsResponse> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-assignments`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al cargar asignaciones"))
  return res.json()
}

export async function saveDispatchPlanPickingAssignments(
  planId: number,
  assignments: {
    related_document_id: number
    load_batch_id: number | null
    oc_document_id?: number | null
    document_number?: number | null
    client_name?: string | null
    document_total?: number | null
  }[],
): Promise<DispatchPlanPickingAssignmentsResponse> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-assignments`,
    {
      method: "PUT",
      headers: getAuthHeaders(),
      body: JSON.stringify({ assignments }),
    },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al guardar asignaciones"))
  return res.json()
}

export async function searchDispatchPlanOrders(
  planId: number,
  q: string,
): Promise<{ dispatch_plan_id: number; q: string; items: DispatchPlanOrderSearchHit[] }> {
  const qs = new URLSearchParams({ q })
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/orders/search?${qs}`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al buscar OC"))
  return res.json()
}

export async function previewAddDispatchPlanOrder(planId: number): Promise<{
  dispatch_plan_id: number
  can_add: boolean
  blocked_reason?: string | null
  has_picking: boolean
  warning?: string | null
}> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/orders/add-preview`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al validar plan"))
  return res.json()
}

export async function addDispatchPlanOrder(
  planId: number,
  body: { oc_document_id: number; regenerate_picking?: boolean; reason?: string },
): Promise<{
  added?: boolean
  requires_regenerate?: boolean
  warning?: string | null
  picking?: unknown
  plan?: { plan: DispatchPlanSummary }
}> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/orders/add`,
    {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(body),
    },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al agregar OC"))
  return res.json()
}

export async function getDispatchPlanPickingRegenerationLog(
  planId: number,
): Promise<{ dispatch_plan_id: number; items: DispatchPlanOrderEvent[] }> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-regeneration-log`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) throw new Error(await res.text().catch(() => "Error al cargar historial"))
  return res.json()
}

export async function markDispatchPlanPickingGenerated(planId: number): Promise<unknown> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/picking-generated`,
    { method: "POST", headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al actualizar estado del plan")
  }
  return res.json()
}

export type DispatchPlanCuadraturaResult = {
  resumen_pagos?: Record<string, number>
  notas_credito_clp: number
  no_cargados_clp: number
  venta_ajustada_clp: number
  total_recaudado_clp: number
  diferencia_clp: number
  diferencia_status: "green" | "yellow" | "red"
}

export type DispatchPlanCuadraturaDocumentRow = {
  related_document_id?: number | null
  document_number?: number | null
  oc_document_id?: number | null
  client_name?: string
  monto_clp: number
  medio_pago: string
  observacion?: string
  route_order?: number | null
}

export type DispatchPlanCuadraturaCreditNoteV2 = {
  documento: string
  nota_credito: string
  monto: number
  observacion: string
}

export type DispatchPlanCuadraturaNotLoadedV2 = {
  producto: string
  producto_variante?: string
  cantidad: number
  motivo: string
  product_id?: number | null
  variant_id?: number | null
  codigo_barras?: string | null
}

export type DispatchPlanCuadraturaCashCountRow = {
  denominacion_clp: number
  cantidad: number
  subtotal_clp: number
}

export type DispatchPlanCuadraturaResponse = {
  dispatch_plan_id: number
  schema_version?: number
  picking_ready?: boolean
  picking_id?: number | null
  picking_version?: number | null
  ventas: {
    venta_oc_clp: number
    venta_facturada_clp: number
    venta_picking_clp: number
  }
  documents?: DispatchPlanCuadraturaDocumentRow[]
  resumen_pagos?: Record<string, number>
  resumen_pagos_labels?: Record<string, string>
  medios_pago_options?: string[]
  credit_notes_v2?: DispatchPlanCuadraturaCreditNoteV2[]
  not_loaded_v2?: DispatchPlanCuadraturaNotLoadedV2[]
  cash_count?: DispatchPlanCuadraturaCashCountRow[]
  product_catalog?: {
    product_id?: number | null
    variant_id?: number | null
    producto: string
    variante?: string
    producto_variante: string
    codigo_barras?: string | null
  }[]
  observacion?: string | null
  operational_status?: string
  operational_status_label?: string
  closed_at?: string | null
  closed_by?: string | null
  resultado: DispatchPlanCuadraturaResult
  observacion_required: boolean
  history?: {
    id: number
    version: number
    status: string
    closed_at?: string
    closed_by?: string | null
    observacion?: string | null
    diferencia_clp?: number
    diferencia_status?: string
  }[]
  analytics_meta?: Record<string, unknown>
  /** Legacy v1 */
  pagos?: {
    transferencia_clp: number
    efectivo_clp: number
    cheque_clp: number
    debito_clp: number
  }
  credit_notes?: {
    documento_venta: string
    nota_credito: string
    monto: number
    motivo: string
  }[]
  not_loaded?: {
    cliente: string
    documento: string
    monto: number
    motivo: string
  }[]
  legacy?: Record<string, unknown>
}

export type CuadraturaListItem = {
  dispatch_plan_id: number
  planning_code?: string
  planning_name?: string
  planning_date?: string
  truck_name?: string
  route_name?: string
  driver_name?: string
  venta_picking_clp?: number
  total_recaudado_clp?: number
  diferencia_clp?: number
  diferencia_status?: string
  cuadratura_status?: string
  operational_status_label?: string
  closed_at?: string | null
}

export async function getDispatchPlanCuadratura(
  planId: number,
): Promise<DispatchPlanCuadraturaResponse> {
  const res = await fetch(`${API_URL}/distribuidora/dispatch-plans/${planId}/cuadratura`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar cuadratura")
  }
  return res.json() as Promise<DispatchPlanCuadraturaResponse>
}

export async function putDispatchPlanCuadratura(
  planId: number,
  body: {
    schema_version?: number
    observacion?: string | null
    documents?: DispatchPlanCuadraturaDocumentRow[]
    credit_notes_v2?: DispatchPlanCuadraturaCreditNoteV2[]
    not_loaded_v2?: DispatchPlanCuadraturaNotLoadedV2[]
    cash_count?: DispatchPlanCuadraturaCashCountRow[]
    transferencia_clp?: number
    efectivo_clp?: number
    cheque_clp?: number
    debito_clp?: number
    credit_notes?: DispatchPlanCuadraturaResponse["credit_notes"]
    not_loaded?: DispatchPlanCuadraturaResponse["not_loaded"]
  },
): Promise<DispatchPlanCuadraturaResponse> {
  const res = await fetch(`${API_URL}/distribuidora/dispatch-plans/${planId}/cuadratura`, {
    method: "PUT",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al guardar cuadratura")
  }
  return res.json() as Promise<DispatchPlanCuadraturaResponse>
}

export async function closeDispatchPlanCuadratura(
  planId: number,
  body?: { observacion?: string | null; closed_by?: string | null },
): Promise<DispatchPlanCuadraturaResponse> {
  const res = await fetch(`${API_URL}/distribuidora/dispatch-plans/${planId}/cuadratura/close`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body ?? {}),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cerrar cuadratura")
  }
  return res.json() as Promise<DispatchPlanCuadraturaResponse>
}

export async function listDistribuidoraCuadraturas(params?: {
  status?: "all" | "pending" | "squared" | "with_diff"
  search?: string
  limit?: number
  offset?: number
}): Promise<{ items: CuadraturaListItem[]; count: number }> {
  const qs = new URLSearchParams()
  if (params?.status) qs.set("status", params.status)
  if (params?.search?.trim()) qs.set("search", params.search.trim())
  if (params?.limit != null) qs.set("limit", String(params.limit))
  if (params?.offset != null) qs.set("offset", String(params.offset))
  const res = await fetch(`${API_URL}/distribuidora/cuadraturas?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al listar cuadraturas")
  }
  return res.json() as Promise<{ items: CuadraturaListItem[]; count: number }>
}

/** Fila de GET /distribuidora/orders/purchase/by-document-ids (preview planificación). */
export type DistribuidoraPlanningPreviewItem = {
  document_id: number
  oc_number?: number | null
  client_id?: number | null
  client_name?: string | null
  municipality?: string | null
  address?: string | null
  total_amount?: number | null
  seller?: string | null
  lat?: number | null
  lon?: number | null
}

export async function getDistribuidoraPurchaseByDocumentIds(params: {
  documentIds: number[]
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraPlanningPreviewItem[] }> {
  const ids = params.documentIds.filter((x) => Number.isFinite(x) && x > 0)
  if (ids.length === 0) return { items: [] }
  const qs = new URLSearchParams()
  qs.set("ids", ids.join(","))
  const res = await fetch(
    `${API_URL}/distribuidora/orders/purchase/by-document-ids?${qs}`,
    {
      headers: getAuthHeaders(),
      signal: params.signal,
    },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar órdenes por id")
  }
  return res.json() as Promise<{ items: DistribuidoraPlanningPreviewItem[] }>
}

export type DistribuidoraRoutePlanningBatchResponse = {
  inserted: number
  planning_date: string
  items: Array<Record<string, unknown>>
  summaries: Array<Record<string, unknown>>
  total_clients: number
  total_amount: number
}

export async function postDistribuidoraRoutePlanningBatch(body: {
  planning_date: string
  assignments: { document_id: number; truck: string }[]
}): Promise<DistribuidoraRoutePlanningBatchResponse> {
  const res = await fetch(`${API_URL}/distribuidora/route-planning/batch`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al confirmar planificación")
  }
  return res.json() as Promise<DistribuidoraRoutePlanningBatchResponse>
}

/** GET /distribuidora/sales (planificación / análisis). */
export type DistribuidoraSalesItem = {
  document_id?: number
  document_type_id?: number | null
  emission_date?: string | null
  client_id?: number | null
  municipality?: string | null
  seller_name?: string | null
  /** Neto (factura/boleta + NC negativas). */
  total_amount_net?: number | null
  /** Solo monto de boleta/factura; 0 en NC. */
  total_amount_sales?: number | null
  /** 1 si es venta (tipo 1 o 6), 0 si es NC. */
  is_sale?: number | null
  /** Igual que ``total_amount_net`` (compatibilidad). */
  total_amount?: number | null
  [key: string]: unknown
}

export type DistribuidoraSalesResponse = {
  total: number
  limit: number
  offset: number
  items: DistribuidoraSalesItem[]
}

export async function getDistribuidoraSales(params: {
  start_date?: string
  end_date?: string
  seller?: string
  municipality?: string
  limit?: number
  offset?: number
  signal?: AbortSignal
}): Promise<DistribuidoraSalesResponse> {
  const qs = new URLSearchParams()
  if (params.start_date?.trim()) qs.set("start_date", params.start_date.trim())
  if (params.end_date?.trim()) qs.set("end_date", params.end_date.trim())
  if (params.seller?.trim()) qs.set("seller", params.seller.trim())
  if (params.municipality?.trim()) qs.set("municipality", params.municipality.trim())
  qs.set("limit", String(Math.min(params.limit ?? 1000, 5000)))
  qs.set("offset", String(params.offset ?? 0))
  const res = await fetch(`${API_URL}/distribuidora/sales?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar ventas")
  }
  return res.json() as Promise<DistribuidoraSalesResponse>
}

/** Análisis de clientes (GET /distribuidora/clients*). */
export type DistribuidoraClientConsolidated = {
  client_id: number
  client_name?: string | null
  total_compras?: number
  total_comprado?: number
  ticket_promedio?: number
  primera_compra?: string | null
  ultima_compra?: string | null
  /** Vendedor de la última venta (tipos 1/6) en el período. */
  vendedor?: string | null
}

export type DistribuidoraClientsConsolidatedResponse = {
  items: DistribuidoraClientConsolidated[]
  limit: number
  offset: number
}

/** GET /distribuidora/clientes/analisis — comportamiento, frecuencia mensual, nivel A–E. */
export type DistribuidoraClienteAnalisis = {
  client_id: number
  nombre?: string | null
  fantasy_name?: string | null
  rut_clean?: string | null
  municipality?: string | null
  city?: string | null
  ultima_compra?: string | null
  compra_30_dias?: number
  compra_60_dias?: number
  freq_enero?: number
  freq_febrero?: number
  freq_marzo?: number
  freq_abril?: number
  nivel_cliente?: string
  dias_sin_comprar?: number | null
}

export async function getDistribuidoraClientesAnalisis(params?: {
  limit?: number
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraClienteAnalisis[] }> {
  const qs = new URLSearchParams()
  qs.set("limit", String(Math.min(params?.limit ?? 5000, 10000)))
  const res = await fetch(`${API_URL}/distribuidora/clientes/analisis?${qs}`, {
    headers: getAuthHeaders(),
    signal: params?.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar análisis de clientes")
  }
  return res.json() as Promise<{ items: DistribuidoraClienteAnalisis[] }>
}

/** Descarga GET /distribuidora/clientes/analisis/export (.xlsx). */
export async function downloadDistribuidoraClientesAnalisisExcel(params?: {
  limit?: number
}): Promise<void> {
  const qs = new URLSearchParams()
  qs.set("limit", String(Math.min(params?.limit ?? 10000, 10000)))
  const res = await fetch(`${API_URL}/distribuidora/clientes/analisis/export?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al exportar análisis")
  }
  const blob = await res.blob()
  const cd = res.headers.get("Content-Disposition")
  const m = cd?.match(/filename="([^"]+)"/)
  const name = m?.[1] ?? "analisis_clientes.xlsx"
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export async function getDistribuidoraClientsConsolidated(params: {
  start_date?: string
  end_date?: string
  seller?: string
  municipality?: string
  limit?: number
  offset?: number
  signal?: AbortSignal
}): Promise<DistribuidoraClientsConsolidatedResponse> {
  const qs = new URLSearchParams()
  if (params.start_date?.trim()) qs.set("start_date", params.start_date.trim())
  if (params.end_date?.trim()) qs.set("end_date", params.end_date.trim())
  if (params.seller?.trim()) qs.set("seller", params.seller.trim())
  if (params.municipality?.trim()) qs.set("municipality", params.municipality.trim())
  qs.set("limit", String(Math.min(params.limit ?? 1000, 5000)))
  qs.set("offset", String(params.offset ?? 0))
  const res = await fetch(`${API_URL}/distribuidora/clients?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar clientes consolidados")
  }
  return res.json() as Promise<DistribuidoraClientsConsolidatedResponse>
}

export type DistribuidoraClientTop = {
  client_id: number
  client_name?: string | null
  total?: number
}

export async function getDistribuidoraClientsTop(params: {
  limit?: number
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraClientTop[] }> {
  const qs = new URLSearchParams()
  qs.set("limit", String(Math.min(params.limit ?? 20, 1000)))
  const res = await fetch(`${API_URL}/distribuidora/clients/top?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar top clientes")
  }
  return res.json() as Promise<{ items: DistribuidoraClientTop[] }>
}

export type DistribuidoraClientInactive = {
  client_id: number
  client_name?: string | null
  ultima_compra?: string | null
  dias_sin_comprar?: number
  vendedor?: string | null
}

export async function getDistribuidoraClientsInactive(params: {
  days?: number
  limit?: number
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraClientInactive[] }> {
  const qs = new URLSearchParams()
  qs.set("days", String(Math.max(1, params.days ?? 7)))
  qs.set("limit", String(Math.min(params.limit ?? 1000, 5000)))
  const res = await fetch(`${API_URL}/distribuidora/clients/inactive?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar clientes inactivos")
  }
  return res.json() as Promise<{ items: DistribuidoraClientInactive[] }>
}

export type DistribuidoraClientFrequency = {
  client_id: number
  client_name?: string | null
  compras?: number
  frecuencia_dias?: number
}

export async function getDistribuidoraClientsFrequency(params: {
  start_date?: string
  end_date?: string
  seller?: string
  municipality?: string
  limit?: number
  signal?: AbortSignal
}): Promise<{ items: DistribuidoraClientFrequency[] }> {
  const qs = new URLSearchParams()
  if (params.start_date?.trim()) qs.set("start_date", params.start_date.trim())
  if (params.end_date?.trim()) qs.set("end_date", params.end_date.trim())
  if (params.seller?.trim()) qs.set("seller", params.seller.trim())
  if (params.municipality?.trim()) qs.set("municipality", params.municipality.trim())
  qs.set("limit", String(Math.min(params.limit ?? 1000, 5000)))
  const res = await fetch(`${API_URL}/distribuidora/clients/frequency?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar frecuencia")
  }
  return res.json() as Promise<{ items: DistribuidoraClientFrequency[] }>
}

export type DistribuidoraClientSellerSummary = {
  seller_name?: string | null
  clientes?: number
  ventas?: number
  ticket_promedio?: number
  clientes_inactivos?: number
}

export type DistribuidoraDailySale = {
  day?: string | null
  total_net?: number | null
}

export type DistribuidoraRecoverClient = {
  client_id: number
  client_name?: string | null
  vendedor?: string | null
  ultima_compra?: string | null
  dias_sin_comprar?: number
  valor_historico_neto?: number | null
}

export type DistribuidoraClientsDashboardResponse = {
  chart_range: { start: string; end: string; days: number }
  kpi_month: { year: number; month: number; label: string }
  daily_sales: DistribuidoraDailySale[]
  sales_by_seller: DistribuidoraClientSellerSummary[]
  seller_totals: { sellers: number; ventas_total: number }
  kpis: {
    ventas_mes?: number | null
    ticket_mes?: number | null
    clientes_activos?: number | null
  }
  recover_clients: DistribuidoraRecoverClient[]
}

export async function getDistribuidoraClientsDashboard(params: {
  chart_days?: number
  kpi_year?: number
  kpi_month?: number
  recover_min_days?: number
  signal?: AbortSignal
}): Promise<DistribuidoraClientsDashboardResponse> {
  const qs = new URLSearchParams()
  if (params.chart_days != null) qs.set("chart_days", String(params.chart_days))
  if (params.kpi_year != null) qs.set("kpi_year", String(params.kpi_year))
  if (params.kpi_month != null) qs.set("kpi_month", String(params.kpi_month))
  if (params.recover_min_days != null) qs.set("recover_min_days", String(params.recover_min_days))
  const res = await fetch(`${API_URL}/distribuidora/clients/dashboard?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar dashboard comercial")
  }
  return res.json() as Promise<DistribuidoraClientsDashboardResponse>
}

export type DistribuidoraClientsSummarySellersResponse = {
  items: DistribuidoraClientSellerSummary[]
  totals: { sellers: number; ventas_total: number }
}

export async function getDistribuidoraClientsSummarySellers(params: {
  limit?: number
  start_date?: string
  end_date?: string
  /** IDs Bsale separados por coma (ej. 80,85,59,89). */
  seller_ids?: string
  signal?: AbortSignal
}): Promise<DistribuidoraClientsSummarySellersResponse> {
  const qs = new URLSearchParams()
  qs.set("limit", String(Math.min(params.limit ?? 500, 5000)))
  if (params.start_date?.trim()) qs.set("start_date", params.start_date.trim())
  if (params.end_date?.trim()) qs.set("end_date", params.end_date.trim())
  if (params.seller_ids?.trim()) qs.set("seller_ids", params.seller_ids.trim())
  const res = await fetch(`${API_URL}/distribuidora/clients/summary/sellers?${qs}`, {
    headers: getAuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar resumen por vendedor")
  }
  return res.json() as Promise<DistribuidoraClientsSummarySellersResponse>
}

/** Filas genéricas JSON (listados /distribuidora/*). */
export type DistribuidoraRecord = Record<string, unknown>

/** Fila de GET /distribuidora/rutero?vendedor=&dia= */
export type DistribuidoraRuteroFila = {
  id: number
  vendedor?: string | null
  dia_atencion?: string | null
  /** Atención sábado cuando vale ``sabado`` (persistido en ``dia_extra``). */
  dia_extra?: string | null
  orden_manual?: number | null
  orden_ruta?: number | null
  rut?: string | null
  bsale_id?: number | null
  nombre_fantasia?: string | null
  razon_social?: string | null
  direccion?: string | null
  municipality?: string | null
  cliente_nombre?: string | null
  lat?: number | null
  lon?: number | null
  telefono?: string | null
  observaciones?: string | null
  tipo_atencion?: string | null
  activo?: boolean | null
}

/** Query GET /distribuidora/rutero — campos vacíos = sin filtro (todos). */
export type DistribuidoraRuteroQuery = {
  vendedor?: string
  dia?: string
  tipo?: "terreno" | "telefonico"
  geo?: "con" | "sin"
  /** `con` = con dia_atencion asignado; `sin` = sin día (ignora filtro `dia` por día de semana). */
  dia_estado?: "con" | "sin"
  /** `con` = dia_extra sábado; `sin` = sin marca sábado. */
  sabado?: "con" | "sin"
}

export async function getDistribuidoraRutero(
  query: DistribuidoraRuteroQuery,
  signal?: AbortSignal,
): Promise<DistribuidoraRuteroFila[]> {
  const qs = new URLSearchParams()
  const v = query.vendedor?.trim()
  const d = query.dia?.trim()
  if (v) qs.set("vendedor", v)
  if (d) qs.set("dia", d)
  if (query.tipo) qs.set("tipo", query.tipo)
  if (query.geo) qs.set("geo", query.geo)
  if (query.dia_estado) qs.set("dia_estado", query.dia_estado)
  if (query.sabado) qs.set("sabado", query.sabado)
  const res = await fetch(`${API_URL}/distribuidora/rutero?${qs}`, {
    headers: getAuthHeaders(),
    signal,
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar rutero")
  }
  const data = await res.json()
  return Array.isArray(data) ? (data as DistribuidoraRuteroFila[]) : []
}

/** POST /distribuidora/observacion — `cliente_id` = PK `bsale.rutero.id`. */
export async function postDistribuidoraObservacionRutero(body: {
  cliente_id: number
  observaciones: string | null
}): Promise<DistribuidoraRuteroFila> {
  const res = await fetch(`${API_URL}/distribuidora/observacion`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo guardar observaciones")
  }
  return res.json() as Promise<DistribuidoraRuteroFila>
}

/** PATCH /distribuidora/rutero/{id} — `id` = PK `bsale.rutero.id`. */
export async function patchDistribuidoraRuteroTipoAtencion(
  ruteroId: number,
  body: { tipo_atencion: "TERRENO" | "TELEFONICO" },
): Promise<DistribuidoraRuteroFila> {
  const res = await fetch(`${API_URL}/distribuidora/rutero/${ruteroId}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo actualizar el tipo de atención")
  }
  return res.json() as Promise<DistribuidoraRuteroFila>
}

/** PATCH /distribuidora/rutero/sabado — ``dia_extra`` = ``sabado`` o NULL por RUT. */
export async function patchDistribuidoraRuteroSabado(body: {
  rut_clean: string
  activo: boolean
}): Promise<{ updated: number; rut_clean: string; activo: boolean }> {
  const res = await fetch(`${API_URL}/distribuidora/rutero/sabado`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo actualizar sábado")
  }
  return res.json() as Promise<{ updated: number; rut_clean: string; activo: boolean }>
}

/**
 * @deprecated La UI de pendientes se unificó en `/distribuidora/rutero`. El endpoint backend sigue
 *   disponible por compatibilidad; no uses esta función en pantallas nuevas.
 */
export async function getDistribuidoraPendientes(): Promise<DistribuidoraRecord[]> {
  const res = await fetch(`${API_URL}/distribuidora/pendientes`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar pendientes")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

/**
 * POST /distribuidora/pendientes/asignar-dia — actualiza `dia_atencion` en bsale.clients.
 * @deprecated Preferir flujos desde Rutero; endpoint conservado por compatibilidad.
 */
export async function postDistribuidoraPendientesAsignarDia(body: {
  bsale_id: number
  dia_atencion: string
}): Promise<DistribuidoraRecord> {
  const res = await fetch(`${API_URL}/distribuidora/pendientes/asignar-dia`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "No se pudo asignar el día")
  }
  return res.json() as Promise<DistribuidoraRecord>
}

/**
 * @deprecated La vista “sin georef” se unificó en Rutero. El endpoint backend sigue disponible.
 */
export async function getDistribuidoraSinGeoref(): Promise<DistribuidoraRecord[]> {
  const res = await fetch(`${API_URL}/distribuidora/sin-georef`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar registros sin georreferencia")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

/**
 * Descarga GET /distribuidora/sin-georef/export (Excel .xlsx).
 * @deprecated Si se reexpone exportación, hacerlo desde Rutero; función conservada por compatibilidad.
 */
export async function downloadDistribuidoraSinGeorefExcel(): Promise<void> {
  const res = await fetch(`${API_URL}/distribuidora/sin-georef/export`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al exportar Excel")
  }
  const blob = await res.blob()
  const url = URL.createObjectURL(blob)
  try {
    if (typeof document === "undefined") {
      throw new Error("La descarga solo está disponible en el navegador.")
    }
    const a = document.createElement("a")
    a.href = url
    a.download = "sin_georef.xlsx"
    a.rel = "noopener"
    document.body.appendChild(a)
    a.click()
    a.remove()
  } finally {
    URL.revokeObjectURL(url)
  }
}

export async function getSuppliers(name?: string): Promise<Supplier[]> {
  const qs = new URLSearchParams()
  const companyId = getCompanyId()
  if (companyId != null && Number.isFinite(companyId) && companyId > 0) {
    qs.set("company_id", String(companyId))
  }
  if (name && name.trim()) {
    qs.set("name", name.trim())
  }

  const url = qs.toString() ? `${API_URL}/suppliers?${qs.toString()}` : `${API_URL}/suppliers`
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    throw new Error("Error al cargar proveedores")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export async function createSupplier(payload: CreateSupplierPayload): Promise<Supplier> {
  const qs = new URLSearchParams()
  const companyId = getCompanyId()
  if (companyId != null && Number.isFinite(companyId) && companyId > 0) {
    qs.set("company_id", String(companyId))
  }
  const url = qs.toString() ? `${API_URL}/suppliers?${qs.toString()}` : `${API_URL}/suppliers`

  const res = await fetch(url, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al crear proveedor")
  }
  return res.json()
}

export async function updateSupplier(
  supplierId: number,
  payload: UpdateSupplierPayload,
): Promise<Supplier> {
  const qs = new URLSearchParams()
  const companyId = getCompanyId()
  if (companyId != null && Number.isFinite(companyId) && companyId > 0) {
    qs.set("company_id", String(companyId))
  }
  const base = `${API_URL}/suppliers/${supplierId}`
  const url = qs.toString() ? `${base}?${qs.toString()}` : base

  const res = await fetch(url, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al actualizar proveedor")
  }
  return res.json()
}

function resolvePurchaseCompanyId(override?: number | null): number {
  if (override != null && Number.isFinite(override) && override > 0) {
    return override
  }
  const companyId = getCompanyId()
  if (companyId == null || !Number.isFinite(companyId)) {
    throw new Error("Empresa no seleccionada")
  }
  return companyId
}

export async function getPurchaseDataFreshness(companyId: number): Promise<PurchaseDataFreshness> {
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  const res = await fetch(`${API_URL}/purchase-data-freshness?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("No se pudo cargar el estado de los datos")
  }
  return res.json() as Promise<PurchaseDataFreshness>
}

export async function getPurchaseOffices(companyId: number): Promise<PurchaseOfficeRef[]> {
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  const res = await fetch(`${API_URL}/purchase-offices?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar sucursales")
  }
  const data = await res.json()
  const list = Array.isArray(data) ? data : []
  return list.map(
    (o: {
      office_id?: number
      office_name?: string | null
      office_state?: number | null
      is_active?: boolean | null
      label?: string
    }) => {
      const id = Number(o.office_id)
      const rawSt = o.office_state
      const office_state =
        rawSt == null || !Number.isFinite(Number(rawSt)) ? null : Number(rawSt)
      const is_active =
        typeof o.is_active === "boolean"
          ? o.is_active
          : office_state === null
            ? null
            : office_state === 0
      const label =
        typeof o.label === "string" && o.label.trim() ? o.label.trim() : `Sucursal ${id}`
      return {
        office_id: id,
        office_name: o.office_name ?? null,
        office_state,
        is_active,
        label,
      }
    },
  )
}

export async function getPurchaseAnalysis(params: {
  companyId: number
  officeId: number
  supplierId?: number
}): Promise<PurchaseAnalysisRow[]> {
  const qs = new URLSearchParams()
  qs.set("company_id", String(params.companyId))
  qs.set("office_id", String(params.officeId))
  if (params.supplierId != null && Number.isFinite(params.supplierId)) {
    qs.set("supplier_id", String(params.supplierId))
  }
  const res = await fetch(`${API_URL}/purchase-analysis?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al cargar análisis de compra")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export async function getPurchaseOrders(options?: {
  companyId?: number
  officeId?: number
}): Promise<PurchaseOrderHeader[]> {
  const companyId = resolvePurchaseCompanyId(options?.companyId ?? null)
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  if (options?.officeId != null && Number.isFinite(options.officeId)) {
    qs.set("office_id", String(options.officeId))
  }
  const res = await fetch(`${API_URL}/purchase-orders?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar órdenes de compra")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export async function getPurchaseOrder(
  ocId: number,
  options?: { companyId?: number },
): Promise<{
  header: PurchaseOrderHeader
  details: PurchaseOrderDetailRow[]
}> {
  const companyId = resolvePurchaseCompanyId(options?.companyId ?? null)
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  const res = await fetch(`${API_URL}/purchase-orders/${ocId}?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al cargar la OC")
  }
  return res.json()
}

export async function generatePurchaseOrder(
  payload: GeneratePurchaseOrderPayload,
): Promise<{ oc_id: number }> {
  const res = await fetch(`${API_URL}/purchase-orders/generate`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al generar la OC")
  }
  return res.json()
}

export async function generatePurchaseOrderFromLines(
  payload: GeneratePurchaseOrderFromLinesPayload,
): Promise<{ oc_id: number }> {
  const res = await fetch(`${API_URL}/purchase-orders/generate-from-lines`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al generar la OC")
  }
  return res.json()
}

export async function patchPurchaseOrderStatus(
  ocId: number,
  status: string,
  companyId?: number,
): Promise<{ oc_id: number; status: string }> {
  const cid = resolvePurchaseCompanyId(companyId ?? null)
  const qs = new URLSearchParams()
  qs.set("company_id", String(cid))
  const res = await fetch(`${API_URL}/purchase-orders/${ocId}?${qs.toString()}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ status }),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al actualizar estado")
  }
  return res.json()
}

export async function getPurchaseManualItems(
  officeId: number,
  supplierId?: number,
): Promise<PurchaseManualItem[]> {
  const companyId = getCompanyId()
  if (companyId == null || !Number.isFinite(companyId)) {
    throw new Error("Empresa no seleccionada")
  }
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  qs.set("office_id", String(officeId))
  if (supplierId != null && Number.isFinite(supplierId)) {
    qs.set("supplier_id", String(supplierId))
  }
  const res = await fetch(`${API_URL}/purchase-manual-items?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar ítems manuales")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export async function createPurchaseManualItem(
  payload: CreatePurchaseManualItemPayload,
): Promise<PurchaseManualItem> {
  const res = await fetch(`${API_URL}/purchase-manual-items`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al crear ítem manual")
  }
  return res.json()
}

export async function deletePurchaseManualItem(itemId: number): Promise<void> {
  const companyId = getCompanyId()
  if (companyId == null || !Number.isFinite(companyId)) {
    throw new Error("Empresa no seleccionada")
  }
  const qs = new URLSearchParams()
  qs.set("company_id", String(companyId))
  const res = await fetch(`${API_URL}/purchase-manual-items/${itemId}?${qs.toString()}`, {
    method: "DELETE",
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al eliminar ítem")
  }
}

/** Tamaño de página por defecto (alineado con el backend). */
export const PRODUCTS_MASTER_PAGE_SIZE = 500

export interface ProductsMasterPage {
  items: ProductMasterRow[]
  total: number
  limit: number
  offset: number
}

export interface GetProductsMasterParams {
  without_supplier?: boolean
  supplier_id?: number
  search?: string
  logistics_incomplete?: boolean
  limit?: number
  offset?: number
}

export type ProductMasterLogisticsPatch = {
  supplier_id?: number | null
  is_active?: boolean
  units_per_box?: number | null
  weight_box_kg?: number | null
  height_cm?: number | null
  width_cm?: number | null
  length_cm?: number | null
  logistics_completed?: boolean
}

export interface ProductsMasterLogisticsStats {
  total: number
  with_barcode: number
  with_units_per_box: number
  with_supplier: number
  with_weight: number
  with_dimensions: number
  logistics_completed: number
  completeness_pct: number
}

export async function getProductsMasterLogisticsStats(): Promise<ProductsMasterLogisticsStats> {
  const res = await fetch(`${API_URL}/products-master/logistics-stats`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar estadísticas logísticas")
  }
  return res.json()
}

export async function patchProductMasterById(
  id: number,
  payload: ProductMasterLogisticsPatch,
): Promise<PatchProductMasterResponse> {
  const res = await fetch(`${API_URL}/products-master/id/${id}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al actualizar producto")
  }
  return res.json()
}

export async function getProductsMaster(
  params?: GetProductsMasterParams,
): Promise<ProductsMasterPage> {
  const qs = new URLSearchParams()
  if (params?.without_supplier) {
    qs.set("without_supplier", "true")
  }
  if (params?.supplier_id != null && Number.isFinite(params.supplier_id)) {
    qs.set("supplier_id", String(Math.trunc(params.supplier_id)))
  }
  if (params?.search != null && params.search.trim()) {
    qs.set("search", params.search.trim())
  }
  if (params?.logistics_incomplete) {
    qs.set("logistics_incomplete", "true")
  }
  const limit = params?.limit ?? PRODUCTS_MASTER_PAGE_SIZE
  const offset = params?.offset ?? 0
  qs.set("limit", String(limit))
  qs.set("offset", String(offset))
  const suffix = qs.toString() ? `?${qs.toString()}` : ""
  const res = await fetch(`${API_URL}/products-master${suffix}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar productos maestros")
  }
  const data = await res.json()
  if (data && Array.isArray(data.items)) {
    return {
      items: data.items,
      total: Number(data.total) || 0,
      limit: Number(data.limit) || limit,
      offset: Number(data.offset) ?? offset,
    }
  }
  return { items: [], total: 0, limit, offset }
}

/** Total de productos con supplier_id IS NULL (COUNT en servidor, sin traer filas). */
export async function getProductsMasterUnassignedCount(): Promise<number> {
  return getProductsMasterWithoutSupplierCount()
}

export interface PatchProductMasterResponse {
  id?: number
  barcode: string
  supplier_id: number | null
  is_active: boolean
  units_per_box?: number | null
  weight_box_kg?: number | null
  height_cm?: number | null
  width_cm?: number | null
  length_cm?: number | null
  weight_unit_kg?: number | null
  volume_m3?: number | null
  logistics_completed?: boolean
  last_bsale_sync_at?: string | null
  updated_at: string | null
}

export async function patchProductMaster(
  barcode: string,
  payload: ProductMasterLogisticsPatch | { supplier_id: number | null },
): Promise<PatchProductMasterResponse> {
  const encoded = encodeURIComponent(barcode)
  const res = await fetch(`${API_URL}/products-master/${encoded}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const msg = await res.text()
    throw new Error(msg || "Error al actualizar producto")
  }
  return res.json()
}

export async function getProductsMasterWithoutSupplier(
  search?: string,
  page?: { limit?: number; offset?: number },
): Promise<ProductsMasterPage> {
  const qs = new URLSearchParams()
  qs.set("supplier_id", "null")
  if (search && search.trim()) {
    qs.set("search", search.trim())
  }
  const limit = page?.limit ?? PRODUCTS_MASTER_PAGE_SIZE
  const offset = page?.offset ?? 0
  qs.set("limit", String(limit))
  qs.set("offset", String(offset))
  const res = await fetch(`${API_URL}/products-master?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar productos sin proveedor")
  }
  const data = await res.json()
  if (data && Array.isArray(data.items)) {
    return {
      items: data.items,
      total: Number(data.total) || 0,
      limit: Number(data.limit) || limit,
      offset: Number(data.offset) ?? offset,
    }
  }
  return { items: [], total: 0, limit, offset }
}

export async function getProductsMasterWithoutSupplierCount(): Promise<number> {
  const res = await fetch(`${API_URL}/products-master/count-without-supplier`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al contar productos sin proveedor")
  }
  const data = await res.json()
  const n = typeof data?.count === "number" ? data.count : Number(data?.count)
  return Number.isFinite(n) ? n : 0
}

/** Fila de GET /promotions/grid */
export interface PromotionGridRow {
  snapshot_id: number
  promotion_id: number
  activa: boolean
  tipo_producto: string
  producto: string
  variante: string
  codigo_barras: string
  descuento_porcentaje: number | string
  descuento_texto: string
  fecha_inicio: string
  fecha_fin: string
  tipo: string
  observacion: string | null
  canal: string
  estado: string
  company_id: number
  price_list: string | null
  /** Precio lista congelado al crear (ANTES) */
  regular_price: number | string
  /** Precio promocional congelado (AHORA) */
  sale_price: number | string
  precio_normal: number | string
  precio_oferta: number | string
  image_url?: string | null
  /** Preparado: etiqueta generada (local hasta backend) */
  has_label_generated?: boolean
}

export interface GetPromotionsGridParams {
  canal?: string
  tipo?: string
  activa?: boolean
  estado?: string
  company_id?: number
}

export async function getPromotionsGrid(
  params?: GetPromotionsGridParams,
): Promise<PromotionGridRow[]> {
  const qs = new URLSearchParams()
  if (params?.canal?.trim()) qs.set("canal", params.canal.trim().toLowerCase())
  if (params?.tipo?.trim()) qs.set("tipo", params.tipo.trim().toLowerCase())
  if (params?.activa !== undefined) qs.set("activa", String(params.activa))
  if (params?.estado?.trim()) qs.set("estado", params.estado.trim())
  if (params?.company_id != null && Number.isFinite(params.company_id)) {
    qs.set("company_id", String(Math.trunc(params.company_id)))
  }
  const suffix = qs.toString() ? `?${qs.toString()}` : ""
  const res = await fetch(`${API_URL}/promotions/grid${suffix}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const t = await res.text()
    throw new Error(t || "Error al cargar promociones")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

export interface PromotionItemPayload {
  barcode: string
  tipo_descuento: "porcentaje" | "precio_fijo"
  valor: number
  observacion?: string | null
}

export interface PromotionCompanyPayload {
  company_id: number
  price_list?: string | null
}

export interface CreatePromotionPayload {
  tipo: "oferta" | "remate" | "promocion"
  canal: "ruta" | "detalle"
  fecha_inicio: string
  fecha_fin: string
  activa?: boolean
  items: PromotionItemPayload[]
  companies: PromotionCompanyPayload[]
}

export async function createPromotion(payload: CreatePromotionPayload): Promise<{
  id: number
  items_processed: number
  snapshots_generated: number
  warnings?: string[]
}> {
  const res = await fetch(`${API_URL}/promotions`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  })
  const text = await res.text()
  if (!res.ok) {
    let msg = "Error al crear la promoción"
    try {
      const j = JSON.parse(text) as { detail?: unknown }
      if (j?.detail != null) {
        msg =
          typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail)
      }
    } catch {
      if (text) msg = text
    }
    throw new Error(msg)
  }
  return JSON.parse(text) as {
    id: number
    items_processed: number
    snapshots_generated: number
    warnings?: string[]
  }
}

/** Snapshot vigente (precios congelados) para etiquetas / consulta puntual */
export interface PromotionActiveSnapshot {
  snapshot_id: number
  promotion_id: number
  barcode: string
  company_id: number
  price_list: string | null
  regular_price: number | string
  sale_price: number | string
  tipo: string
  canal: string
  fecha_inicio: string
  fecha_fin: string
  estado: string
}

export async function getActivePromotionSnapshot(
  companyId: number,
  barcode: string,
): Promise<PromotionActiveSnapshot> {
  const qs = new URLSearchParams({
    company_id: String(companyId),
    barcode: barcode.trim(),
  })
  const res = await fetch(`${API_URL}/promotions/active-snapshot?${qs}`, {
    headers: getAuthHeaders(),
  })
  const text = await res.text()
  if (!res.ok) {
    let msg = "No hay promoción activa para este producto"
    try {
      const j = JSON.parse(text) as { detail?: unknown }
      if (typeof j.detail === "string") msg = j.detail
    } catch {
      if (text) msg = text
    }
    throw new Error(msg)
  }
  return JSON.parse(text) as PromotionActiveSnapshot
}

export async function patchPromotionSnapshotSalePrice(
  snapshotId: number,
  salePrice: number,
): Promise<{
  id: number
  promotion_id: number
  barcode: string
  company_id: number
  price_list: string | null
  regular_price: number | string
  sale_price: number | string
  canal: string
  fecha_generado: string
}> {
  const res = await fetch(`${API_URL}/promotions/snapshots/${snapshotId}/sale-price`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ sale_price: salePrice }),
  })
  const text = await res.text()
  if (!res.ok) {
    let msg = "Error al actualizar precio promocional"
    try {
      const j = JSON.parse(text) as { detail?: unknown }
      if (typeof j.detail === "string") msg = j.detail
    } catch {
      if (text) msg = text
    }
    throw new Error(msg)
  }
  return JSON.parse(text) as {
    id: number
    promotion_id: number
    barcode: string
    company_id: number
    price_list: string | null
    regular_price: number | string
    sale_price: number | string
    canal: string
    fecha_generado: string
  }
}

export async function togglePromotion(promotionId: number): Promise<{
  id: number
  tipo: string
  canal: string
  fecha_inicio: string
  fecha_fin: string
  activa: boolean
  created_at: string
}> {
  const res = await fetch(`${API_URL}/promotions/${promotionId}/toggle`, {
    method: "PUT",
    headers: getAuthHeaders(),
  })
  const text = await res.text()
  if (!res.ok) {
    throw new Error(text || "Error al actualizar promoción")
  }
  return JSON.parse(text) as {
    id: number
    tipo: string
    canal: string
    fecha_inicio: string
    fecha_fin: string
    activa: boolean
    created_at: string
  }
}

export function logout() {
  if (typeof window !== "undefined") {
    localStorage.removeItem("token")
    localStorage.removeItem("email")
    localStorage.removeItem("role")
    localStorage.removeItem("company_id")
    localStorage.removeItem("company_name")
    sessionStorage.removeItem("demo_mode")
    isDemoMode = false
  }
}

export function isAuthenticated(): boolean {
  if (typeof window === "undefined") return false
  return !!localStorage.getItem("token")
}

export function getStoredEmail(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem("email")
}

export function getStoredCompanyName(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem("company_name")
}

/** Producto resuelto para etiquetas de sucursal (GET /labels/product) */
export interface LabelProductResolved {
  variant_id: number
  barcode: string
  sku: string | null
  product_name: string
  variant_name: string | null
  product_type: string | null
  display_name: string
  price: number | null
  price_list_id: number | null
  price_list_name: string | null
  /** Código leído desde Excel (si difiere del barcode en BD) */
  read_barcode?: string | null
  /** Barcode canónico en BD cuando hubo corrección por ceros */
  matched_barcode?: string | null
  tried_barcodes?: string[] | null
  extra_read_barcodes?: string[] | null
}

export async function lookupLabelProduct(
  companyId: number,
  priceListId: number,
  barcode: string,
): Promise<LabelProductResolved | null> {
  const bc = barcode.trim()
  if (!bc) return null
  const params = new URLSearchParams({
    company_id: String(companyId),
    price_list_id: String(priceListId),
    barcode: bc,
  })
  const res = await fetch(`${API_URL}/labels/product?${params}`, {
    headers: getAuthHeaders(),
  })
  if (res.status === 404) return null
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error((err as { detail?: string }).detail || `Error ${res.status}`)
  }
  return res.json()
}

export async function resolveLabelProductsBatch(
  companyId: number,
  priceListId: number,
  items: { barcode: string; quantity?: number }[],
): Promise<{
  resolved: (LabelProductResolved & { quantity: number })[]
  errors: {
    line: number
    barcode: string
    read_barcode?: string
    tried_barcodes?: string[]
    error: string
  }[]
}> {
  const cid = Math.trunc(Number(companyId))
  const plid = Math.trunc(Number(priceListId))
  if (!Number.isFinite(cid) || cid < 1 || !Number.isFinite(plid) || plid < 1) {
    throw new Error("Empresa o lista de precios inválida para resolver etiquetas.")
  }

  const prepared = prepareLabelResolveItems(items)
  if (prepared.length === 0) {
    throw new Error("No hay códigos de barras válidos para importar.")
  }

  const resolved: (LabelProductResolved & { quantity: number })[] = []
  const errors: { line: number; barcode: string; error: string }[] = []
  const chunkSize = 500

  for (let offset = 0; offset < prepared.length; offset += chunkSize) {
    const chunk = prepared.slice(offset, offset + chunkSize)
    const payload = {
      company_id: cid,
      price_list_id: plid,
      items: chunk,
    }
    console.log("labels/resolve payload", payload)

    const res = await fetch(`${API_URL}/labels/resolve`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(payload),
    })
    if (!res.ok) {
      const err = await res.json().catch(() => ({}))
      const detail = (err as { detail?: unknown }).detail
      let msg = `Error ${res.status} al resolver etiquetas`
      if (typeof detail === "string") {
        msg = detail
      } else if (Array.isArray(detail)) {
        msg = detail
          .map((d) => {
            const row = d as { loc?: (string | number)[]; msg?: string }
            return `${(row.loc ?? []).join(".")}: ${row.msg ?? "inválido"}`
          })
          .join("; ")
      }
      throw new Error(msg)
    }
    const data = (await res.json()) as {
      resolved: (LabelProductResolved & { quantity: number })[]
      errors: {
        line: number
        barcode: string
        read_barcode?: string
        tried_barcodes?: string[]
        error: string
      }[]
    }
    for (const item of data.resolved) resolved.push(item)
    for (const item of data.errors) {
      errors.push({
        ...item,
        line: item.line + offset,
      })
    }
  }

  return { resolved, errors }
}

// --- Analítica → Costos ---

export type CostAnalyticsSyncState = {
  company_id: number
  last_admission_ts?: number | null
  last_run_at?: string | null
  last_status?: string | null
  last_message?: string | null
  receptions_inserted?: number
  lines_inserted?: number
  total_lines_processed?: number
}

export type CostOfficeRef = {
  office_id: number
  office_name?: string | null
}

export type CostAnalyticsDashboard = {
  company_id: number
  kpis: {
    variants_total: number
    with_cost: number
    without_cost: number
    zero_cost: number
    receptions_24h: number
    variation_gt_10: number
    variation_gt_20: number
    receptions_processed: number
    lines_processed: number
  }
  last_sync: CostAnalyticsSyncState | null
  offices: CostOfficeRef[]
}

export type CostHistorySearchHit = {
  variant_id: number
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  company_name?: string | null
  average_cost?: number | null
  average_cost_gross?: number | null
}

export type CostHistoryRow = {
  company_id: number
  company_name?: string | null
  office_id?: number | null
  office_name?: string | null
  variant_id: number
  barcode?: string | null
  product_name?: string | null
  variant_name?: string | null
  reception_id: number
  reception_detail_id: number
  document?: string | null
  document_number?: number | null
  admission_date: string
  quantity: number
  cost_net: number
  iva_amount?: number | null
  other_taxes?: number | null
  cost_bruto_erp?: number | null
  average_cost?: number | null
  variation_pct?: number | null
}

export type CostVariantHistory = {
  company_id: number
  variant_id: number
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  average_cost?: number | null
  average_cost_gross?: number | null
  items: CostHistoryRow[]
}

export type CostReceptionRow = {
  reception_id: number
  admission_date: string
  company_name?: string | null
  office_id?: number | null
  office_name?: string | null
  document?: string | null
  document_number?: number | null
  products_count: number
  total_quantity: number
  total_cost_net?: number | null
  total_cost_bruto?: number | null
}

export type CostReceptionDetailLine = {
  reception_detail_id: number
  variant_id: number
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  quantity: number
  cost_net: number
  iva_amount?: number | null
  other_taxes?: number | null
  cost_bruto_erp?: number | null
  average_cost?: number | null
  variation_pct?: number | null
}

export type CostReceptionDetail = CostReceptionRow & {
  items: CostReceptionDetailLine[]
}

export type CostAlertRow = {
  variant_id: number
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  office_id?: number | null
  office_name?: string | null
  cost_net?: number | null
  variation_pct?: number | null
  admission_date?: string | null
  reception_id?: number | null
  average_cost?: number | null
  missing_cost?: boolean
  suspicious_reception?: boolean
  cross_branch_spread?: number | null
  alert_types: string[]
  semaphore: "green" | "yellow" | "red"
}

export type CostOfficeComparison = {
  variant_id: number
  product_name?: string | null
  variant_name?: string | null
  barcode?: string | null
  offices: {
    office_id: number
    office_name?: string | null
    cost_net: number
    cost_bruto_erp?: number | null
    admission_date?: string | null
    reception_id?: number | null
  }[]
  min_cost_net?: number | null
  max_cost_net?: number | null
  max_spread_pct?: number | null
}

function requireCompanyId(): number {
  const id = getCompanyId()
  if (id == null) throw new Error("Seleccione una empresa en el dashboard.")
  return id
}

export async function getCostAnalyticsDashboard(
  params?: {
    company_id?: number
    office_id?: number
    date_from?: string
    date_to?: string
  },
): Promise<CostAnalyticsDashboard> {
  const cid = params?.company_id ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid) })
  if (params?.office_id != null) qs.set("office_id", String(params.office_id))
  if (params?.date_from) qs.set("date_from", params.date_from)
  if (params?.date_to) qs.set("date_to", params.date_to)
  const res = await fetch(`${API_URL}/cost-analytics/dashboard?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar dashboard de costos")
  }
  return res.json() as Promise<CostAnalyticsDashboard>
}

export async function getCostOffices(companyId?: number): Promise<{ items: CostOfficeRef[] }> {
  const cid = companyId ?? requireCompanyId()
  const res = await fetch(`${API_URL}/cost-analytics/offices?company_id=${cid}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) throw new Error("Error al cargar sucursales")
  return res.json() as Promise<{ items: CostOfficeRef[] }>
}

export async function searchCostHistory(
  q: string,
  companyId?: number,
): Promise<{ items: CostHistorySearchHit[]; q: string }> {
  const cid = companyId ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid), q: q.trim() })
  const res = await fetch(`${API_URL}/cost-analytics/history/search?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al buscar historial de costos")
  }
  return res.json() as Promise<{ items: CostHistorySearchHit[]; q: string }>
}

export async function listCostHistory(params?: {
  company_id?: number
  q?: string
  variant_id?: number
  office_id?: number
  date_from?: string
  date_to?: string
  limit?: number
  offset?: number
}): Promise<{ items: CostHistoryRow[]; total: number }> {
  const cid = params?.company_id ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid) })
  if (params?.q?.trim()) qs.set("q", params.q.trim())
  if (params?.variant_id != null) qs.set("variant_id", String(params.variant_id))
  if (params?.office_id != null) qs.set("office_id", String(params.office_id))
  if (params?.date_from) qs.set("date_from", params.date_from)
  if (params?.date_to) qs.set("date_to", params.date_to)
  if (params?.limit != null) qs.set("limit", String(params.limit))
  if (params?.offset != null) qs.set("offset", String(params.offset))
  const res = await fetch(`${API_URL}/cost-analytics/history?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar historial")
  }
  return res.json() as Promise<{ items: CostHistoryRow[]; total: number }>
}

export async function getCostReceptions(params?: {
  company_id?: number
  date_from?: string
  date_to?: string
  office_id?: number
  document_type?: string
  limit?: number
  offset?: number
}): Promise<{ items: CostReceptionRow[]; total: number; limit: number; offset: number }> {
  const cid = params?.company_id ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid) })
  if (params?.date_from) qs.set("date_from", params.date_from)
  if (params?.date_to) qs.set("date_to", params.date_to)
  if (params?.office_id != null) qs.set("office_id", String(params.office_id))
  if (params?.document_type?.trim()) qs.set("document_type", params.document_type.trim())
  if (params?.limit != null) qs.set("limit", String(params.limit))
  if (params?.offset != null) qs.set("offset", String(params.offset))
  const res = await fetch(`${API_URL}/cost-analytics/receptions?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar recepciones")
  }
  return res.json() as Promise<{
    items: CostReceptionRow[]
    total: number
    limit: number
    offset: number
  }>
}

export async function getCostReceptionDetail(
  receptionId: number,
  companyId?: number,
): Promise<CostReceptionDetail> {
  const cid = companyId ?? requireCompanyId()
  const res = await fetch(
    `${API_URL}/cost-analytics/receptions/${receptionId}?company_id=${cid}`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Recepción no encontrada")
  }
  return res.json() as Promise<CostReceptionDetail>
}

export async function getCostVariantHistory(
  variantId: number,
  companyId?: number,
): Promise<CostVariantHistory> {
  const cid = companyId ?? requireCompanyId()
  const res = await fetch(
    `${API_URL}/cost-analytics/history/variants/${variantId}?company_id=${cid}`,
    { headers: getAuthHeaders() },
  )
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar historial de variante")
  }
  return res.json() as Promise<CostVariantHistory>
}

export async function getCostAlerts(
  params?: { company_id?: number; office_id?: number; limit?: number },
): Promise<{ items: CostAlertRow[] }> {
  const cid = params?.company_id ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid) })
  if (params?.office_id != null) qs.set("office_id", String(params.office_id))
  if (params?.limit != null) qs.set("limit", String(params.limit))
  const res = await fetch(`${API_URL}/cost-analytics/alerts?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al cargar alertas de costo")
  }
  return res.json() as Promise<{ items: CostAlertRow[] }>
}

export async function compareCostOffices(params: {
  company_id?: number
  variant_id?: number
  q?: string
}): Promise<
  | { comparison: CostOfficeComparison }
  | { items: CostOfficeComparison[]; q?: string }
> {
  const cid = params.company_id ?? requireCompanyId()
  const qs = new URLSearchParams({ company_id: String(cid) })
  if (params.variant_id != null) qs.set("variant_id", String(params.variant_id))
  if (params.q?.trim()) qs.set("q", params.q.trim())
  const res = await fetch(`${API_URL}/cost-analytics/compare/offices?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al comparar sucursales")
  }
  return res.json()
}

export async function syncCostAnalytics(params?: {
  company_id?: number
  lookback_days?: number
}): Promise<Record<string, unknown>> {
  const qs = new URLSearchParams()
  if (params?.company_id != null) qs.set("company_id", String(params.company_id))
  if (params?.lookback_days != null) qs.set("lookback_days", String(params.lookback_days))
  const suffix = qs.toString() ? `?${qs.toString()}` : ""
  const res = await fetch(`${API_URL}/cost-analytics/sync${suffix}`, {
    method: "POST",
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || "Error al sincronizar costos")
  }
  return res.json() as Promise<Record<string, unknown>>
}
