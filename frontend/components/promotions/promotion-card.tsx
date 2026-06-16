"use client"

import Image from "next/image"
import { Copy, Pencil, Printer } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import type { PromotionGridRow } from "@/lib/api"
import {
  calcDiscountPercent,
  calcSavings,
  formatCurrency,
  formatDiscountBadge,
  formatVigencia,
} from "@/lib/promotions-utils"
import {
  PromotionDiscountBadge,
  PromotionStatusBadge,
  PromotionTipoBadge,
} from "@/components/promotions/promotion-badges"
import { PromotionLabelStatusBadge } from "@/components/promotions/promotion-label-status-badge"

type PromotionCardProps = {
  row: PromotionGridRow
  companyName: string
  onOpen: (row: PromotionGridRow) => void
  onEdit: (row: PromotionGridRow) => void
  onDuplicate: (row: PromotionGridRow) => void
  onLabels: (row: PromotionGridRow) => void
}

export function PromotionCard({
  row,
  companyName,
  onOpen,
  onEdit,
  onDuplicate,
  onLabels,
}: PromotionCardProps) {
  const productName = (row.producto || "").trim()
  const variantName = (row.variante || "").trim()
  const category = (row.tipo_producto || "").trim()
  const discount = formatDiscountBadge(row.regular_price, row.sale_price)
  const discountPct = calcDiscountPercent(row.regular_price, row.sale_price)
  const savings = calcSavings(row.regular_price, row.sale_price)
  const imageUrl = row.image_url?.trim() || null

  return (
    <Card
      className="group cursor-pointer overflow-hidden border shadow-sm transition-shadow hover:shadow-md"
      onClick={() => onOpen(row)}
    >
      <CardContent className="flex h-full flex-col p-0">
        <div className="flex items-start justify-between gap-2 border-b bg-muted/20 px-3 py-2">
          <div className="flex flex-wrap items-center gap-1.5">
            <PromotionStatusBadge estado={row.estado} />
            <PromotionLabelStatusBadge generated={row.has_label_generated} />
          </div>
          <PromotionTipoBadge tipo={row.tipo} />
        </div>

        <div className="relative flex aspect-[4/3] items-center justify-center bg-white p-3">
          {imageUrl ? (
            <Image
              src={imageUrl}
              alt={productName || row.codigo_barras}
              fill
              className="object-contain p-2"
              sizes="(max-width: 640px) 50vw, 20vw"
              unoptimized
            />
          ) : (
            <div className="text-muted-foreground flex h-full w-full items-center justify-center rounded-lg bg-muted/30 text-xs">
              Sin imagen
            </div>
          )}
        </div>

        <div className="flex flex-1 flex-col gap-2.5 p-4">
          {category ? (
            <p className="text-muted-foreground text-[10px] font-semibold uppercase tracking-wider">
              {category}
            </p>
          ) : null}
          <div>
            <p className="text-sm font-bold leading-tight">
              {(productName || "Producto").toUpperCase()}
            </p>
            {variantName && !productName.toLowerCase().includes(variantName.toLowerCase()) ? (
              <p className="text-muted-foreground mt-0.5 text-xs font-medium leading-tight">
                {variantName}
              </p>
            ) : null}
          </div>

          <div className="rounded-lg border bg-muted/20 p-3 space-y-2">
            <div className="flex justify-between gap-2 text-sm">
              <span className="text-muted-foreground font-medium">ANTES</span>
              <span className="text-muted-foreground line-through">
                {formatCurrency(row.regular_price)}
              </span>
            </div>
            <div className="flex justify-between gap-2">
              <span className="text-sm font-semibold text-emerald-800">AHORA</span>
              <span className="text-xl font-bold text-emerald-700">
                {formatCurrency(row.sale_price)}
              </span>
            </div>
            <div className="grid grid-cols-2 gap-2 border-t pt-2 text-xs">
              <div>
                <p className="text-muted-foreground font-medium">AHORRO</p>
                <p className="font-semibold">{formatCurrency(savings)}</p>
              </div>
              <div className="text-right">
                <p className="text-muted-foreground font-medium">DESCUENTO</p>
                <p className="font-bold text-rose-700">
                  {discountPct != null ? `-${discountPct}%` : "—"}
                </p>
              </div>
            </div>
          </div>

          <PromotionDiscountBadge label={discount} className="self-start" />

          <div className="text-muted-foreground mt-auto space-y-0.5 text-xs">
            <p>
              <span className="text-foreground/80 font-medium">Empresa:</span> {companyName}
            </p>
            <p>
              <span className="text-foreground/80 font-medium">Vigencia:</span>{" "}
              {formatVigencia(row.fecha_inicio, row.fecha_fin)}
            </p>
          </div>

          <div className="grid grid-cols-3 gap-1.5 pt-1" onClick={(e) => e.stopPropagation()}>
            <Button type="button" variant="outline" size="sm" className="h-8 px-2 text-xs" onClick={() => onEdit(row)}>
              <Pencil className="mr-1 h-3 w-3" />
              Editar
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 px-2 text-xs"
              onClick={() => onDuplicate(row)}
            >
              <Copy className="mr-1 h-3 w-3" />
              Duplicar
            </Button>
            <Button
              type="button"
              variant="default"
              size="sm"
              className="h-8 px-2 text-xs"
              onClick={() => onLabels(row)}
            >
              <Printer className="mr-1 h-3 w-3" />
              Etiquetas
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
