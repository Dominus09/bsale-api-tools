"use client"

import { cn } from "@/lib/utils"
import {
  formatBarcode,
  formatProductPlusVariant,
  formatProductType,
} from "@/lib/analytics-v2/product-identity"

type Props = {
  productTypeName?: string | null
  productName?: string | null
  variantName?: string | null
  barcode?: string | null
  className?: string
  /** Compacto (móvil): tres líneas densas. */
  compact?: boolean
}

/**
 * Identificación obligatoria en tablas de productos:
 * 1) Tipo  2) Producto — Variante  3) Código de barras (texto).
 */
export function ProductIdentityCell({
  productTypeName,
  productName,
  variantName,
  barcode,
  className,
  compact = false,
}: Props) {
  const typeLabel = formatProductType(productTypeName)
  const title = formatProductPlusVariant(productName, variantName)
  const bars = formatBarcode(barcode)

  return (
    <div className={cn("min-w-0", className)}>
      <p
        className={cn(
          "truncate text-lq-muted",
          compact ? "text-[11px]" : "text-xs",
        )}
        title={typeLabel}
      >
        {typeLabel}
      </p>
      <p
        className={cn(
          "font-medium text-lq-ink group-hover:text-primary",
          compact ? "mt-0.5 text-sm leading-snug" : "mt-0.5 text-sm",
        )}
        title={title}
      >
        <span className="line-clamp-2 break-words">{title}</span>
      </p>
      <p
        className={cn(
          "truncate font-mono tabular-nums text-lq-muted",
          compact ? "mt-0.5 text-[11px]" : "mt-0.5 text-xs",
        )}
        title={bars}
      >
        {bars}
      </p>
    </div>
  )
}
