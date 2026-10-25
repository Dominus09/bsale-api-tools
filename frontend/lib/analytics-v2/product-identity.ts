/** Identificación uniforme de productos en Analítica V2 (tipo · producto+variante · barras). */

export type ProductIdentityFields = {
  productTypeName?: string | null
  productName?: string | null
  variantName?: string | null
  barcode?: string | null
}

export const VARIANT_MISSING_LABEL = "Variante no informada"
export const BARCODE_MISSING_LABEL = "Sin código de barras"
export const TYPE_MISSING_LABEL = "Sin tipo"
export const PRODUCT_MISSING_LABEL = "Producto no informado"

export function formatProductType(name: string | null | undefined): string {
  const t = (name ?? "").trim()
  return t || TYPE_MISSING_LABEL
}

export function formatProductPlusVariant(
  productName: string | null | undefined,
  variantName: string | null | undefined,
): string {
  const product = (productName ?? "").trim()
  const variant = (variantName ?? "").trim()
  if (product && variant) return `${product} — ${variant}`
  if (product && !variant) return `${product} — ${VARIANT_MISSING_LABEL}`
  if (!product && variant) return variant
  return PRODUCT_MISSING_LABEL
}

export function formatBarcode(barcode: string | null | undefined): string {
  const b = barcode == null ? "" : String(barcode).trim()
  return b || BARCODE_MISSING_LABEL
}

export function productIdentity(fields: ProductIdentityFields) {
  return {
    productType: formatProductType(fields.productTypeName),
    productPlusVariant: formatProductPlusVariant(fields.productName, fields.variantName),
    barcode: formatBarcode(fields.barcode),
    hasVariantName: Boolean((fields.variantName ?? "").trim()),
    hasBarcode: Boolean(fields.barcode != null && String(fields.barcode).trim()),
  }
}
