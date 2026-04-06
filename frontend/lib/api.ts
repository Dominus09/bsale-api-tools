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

function getCompanyId(): number | null {
  if (typeof window === "undefined") return null
  const companyId = localStorage.getItem("company_id")
  return companyId ? parseInt(companyId, 10) : null
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
      console.warn("[API] Network error on login, falling back to demo mode")
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

export interface GetProductsMasterParams {
  without_supplier?: boolean
  supplier_id?: number
  search?: string
}

export async function getProductsMaster(
  params?: GetProductsMasterParams,
): Promise<ProductMasterRow[]> {
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
  const suffix = qs.toString() ? `?${qs.toString()}` : ""
  const res = await fetch(`${API_URL}/products-master${suffix}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar productos maestros")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
}

/** GET /products-master?supplier_id=null — total de filas (supplier_id IS NULL). */
export async function getProductsMasterUnassignedCount(): Promise<number> {
  const res = await fetch(`${API_URL}/products-master?supplier_id=null`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al contar productos sin proveedor")
  }
  const data = await res.json()
  return Array.isArray(data) ? data.length : 0
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

export async function getProductsMasterWithoutSupplier(search?: string): Promise<ProductMasterRow[]> {
  const qs = new URLSearchParams()
  qs.set("supplier_id", "null")
  if (search && search.trim()) {
    qs.set("search", search.trim())
  }
  const res = await fetch(`${API_URL}/products-master?${qs.toString()}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) {
    throw new Error("Error al cargar productos sin proveedor")
  }
  const data = await res.json()
  return Array.isArray(data) ? data : []
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
