import { getApiBaseUrl } from "@/lib/api-base"

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
  product_name: string | null
  variant_name: string | null
  product_type: string | null
  supplier_id: number | null
  is_active: boolean
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

function getAuthHeaders(): HeadersInit {
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
  /** Días distintos en rutero (incluye `TELEFONICO` u otros) para selects aunque no vayan al mapa. */
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
}

export type DistribuidoraClientsConsolidatedResponse = {
  items: DistribuidoraClientConsolidated[]
  limit: number
  offset: number
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
}

export type DistribuidoraClientsSummarySellersResponse = {
  items: DistribuidoraClientSellerSummary[]
  totals: { sellers: number; ventas_total: number }
}

export async function getDistribuidoraClientsSummarySellers(params: {
  limit?: number
  signal?: AbortSignal
}): Promise<DistribuidoraClientsSummarySellersResponse> {
  const qs = new URLSearchParams()
  qs.set("limit", String(Math.min(params.limit ?? 500, 5000)))
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
  limit?: number
  offset?: number
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
  barcode: string
  supplier_id: number | null
  is_active: boolean
  updated_at: string | null
}

export async function patchProductMaster(
  barcode: string,
  payload: { supplier_id: number | null },
): Promise<PatchProductMasterResponse> {
  const encoded = encodeURIComponent(barcode)
  const res = await fetch(`${API_URL}/products-master/${encoded}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ supplier_id: payload.supplier_id }),
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
  precio_normal: number | string
  precio_oferta: number | string
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
  tipo: "oferta" | "remate"
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
  return JSON.parse(text) as { id: number; items_processed: number; snapshots_generated: number }
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
