
import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

export type ProductPalletData = { id: number; boxes_per_pallet: number | null; units_per_pallet: number | null; load_weight_kg: number | null }
type PalletPage = { schema_ready: boolean; writes_enabled: boolean; items: ProductPalletData[] }
type PalletStats = { schema_ready: boolean; total: number | null; with_boxes_per_pallet: number | null }

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(getApiBaseUrl() + "/products-master" + path, { ...options, headers: getAuthHeaders(), cache: "no-store" })
  const result = await response.json().catch(() => null)
  if (!response.ok) throw new Error(typeof result?.detail === "string" ? result.detail : "No se pudo consultar o guardar información de pallet.")
  return result as T
}
export function getPalletData(ids: string, signal: AbortSignal) {
  return request<PalletPage>("/pallets?ids=" + encodeURIComponent(ids), { signal })
}
export function getPalletStats(signal: AbortSignal) {
  return request<PalletStats>("/pallets/stats", { signal })
}
export function patchPalletBoxes(id: number, boxes_per_pallet: number | null) {
  return request<ProductPalletData>("/id/" + id + "/pallet", { method: "PATCH", body: JSON.stringify({ boxes_per_pallet }) })
}
