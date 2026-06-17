import { DEFAULT_API_URL } from "@/lib/api-base"

/** CDN de fotos de producto (catálogo Quillotana). */
export const PRODUCT_CDN_BASE =
  process.env.NEXT_PUBLIC_CATALOG_CDN_URL?.trim().replace(/\/$/, "") ||
  "https://cat.quillotana.cl"

export const PRODUCT_LOCAL_IMAGE_BASE = "/products"

export const PRODUCT_IMAGE_PLACEHOLDER = "/icon.svg"

function apiOriginForImages(): string {
  const env = process.env.NEXT_PUBLIC_API_URL?.trim().replace(/\/$/, "")
  return env || DEFAULT_API_URL.replace(/\/$/, "")
}

/**
 * Normaliza image_url del backend (absoluta o relativa).
 * Rutas /products/... se resuelven contra el CDN; otras relativas contra el API.
 */
export function resolveProductImageUrl(url: string | null | undefined): string | null {
  const raw = (url || "").trim()
  if (!raw) return null
  if (raw.startsWith("http://") || raw.startsWith("https://")) return raw
  if (raw.startsWith("/")) {
    if (raw.startsWith(`${PRODUCT_LOCAL_IMAGE_BASE}/`)) {
      return `${PRODUCT_CDN_BASE}${raw}`
    }
    return `${apiOriginForImages()}${raw}`
  }
  return raw
}

export function cdnBarcodeImageUrl(barcode: string | null | undefined): string | null {
  const bc = (barcode || "").trim()
  if (!bc) return null
  return `${PRODUCT_CDN_BASE}${PRODUCT_LOCAL_IMAGE_BASE}/${bc}.webp`
}

export function localBarcodeImageUrl(barcode: string | null | undefined): string | null {
  const bc = (barcode || "").trim()
  if (!bc) return null
  return `${PRODUCT_LOCAL_IMAGE_BASE}/${bc}.webp`
}

/**
 * Cadena de fallback sin duplicados:
 * 1. image_url del catálogo/backend
 * 2. CDN cat.quillotana.cl/products/{barcode}.webp
 * 3. /products/{barcode}.webp (estáticos locales en deploy)
 */
export function productImageFallbackUrls(
  imageUrl: string | null | undefined,
  barcode: string | null | undefined,
): string[] {
  const out: string[] = []
  const push = (u: string | null | undefined) => {
    const v = (u || "").trim()
    if (v && !out.includes(v)) out.push(v)
  }

  push(resolveProductImageUrl(imageUrl))
  push(cdnBarcodeImageUrl(barcode))
  push(localBarcodeImageUrl(barcode))

  return out
}
