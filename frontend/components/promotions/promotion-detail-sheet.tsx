"use client"

import Image from "next/image"
import { Pencil, Printer } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import type { PromotionGridRow } from "@/lib/api"
import {
  calcDiscountPercent,
  calcSavings,
  formatCurrency,
  formatDateShort,
  formatVigencia,
  productDisplayName,
} from "@/lib/promotions-utils"
import {
  PromotionDiscountBadge,
  PromotionStatusBadge,
  PromotionTipoBadge,
} from "@/components/promotions/promotion-badges"
import { PromotionLabelStatusBadge } from "@/components/promotions/promotion-label-status-badge"

type PromotionDetailSheetProps = {
  row: PromotionGridRow | null
  open: boolean
  companyName: string
  onOpenChange: (open: boolean) => void
  onEditSalePrice: (row: PromotionGridRow) => void
  onLabels: (row: PromotionGridRow) => void
}

export function PromotionDetailSheet({
  row,
  open,
  companyName,
  onOpenChange,
  onEditSalePrice,
  onLabels,
}: PromotionDetailSheetProps) {
  if (!row) return null

  const discountPct = calcDiscountPercent(row.regular_price, row.sale_price)
  const discount =
    discountPct != null ? `-${discountPct}%` : "—"
  const savings = calcSavings(row.regular_price, row.sale_price)
  const imageUrl = row.image_url?.trim() || null
  const productName = (row.producto || "").trim()
  const variantName = (row.variante || "").trim()

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle className="text-left">{productDisplayName(row)}</SheetTitle>
          <SheetDescription className="text-left">
            Detalle comercial · precios congelados
          </SheetDescription>
        </SheetHeader>

        <div className="mt-6 space-y-6">
          <div className="flex flex-wrap items-center gap-2">
            <PromotionStatusBadge estado={row.estado} />
            <PromotionTipoBadge tipo={row.tipo} />
            <PromotionLabelStatusBadge generated={row.has_label_generated} />
            <PromotionDiscountBadge label={discount} />
          </div>

          <div className="relative mx-auto aspect-square w-full max-w-[240px] rounded-xl border bg-white p-4">
            {imageUrl ? (
              <Image
                src={imageUrl}
                alt={productDisplayName(row)}
                fill
                className="object-contain p-2"
                sizes="240px"
                unoptimized
              />
            ) : (
              <div className="text-muted-foreground flex h-full items-center justify-center text-sm">
                Sin imagen
              </div>
            )}
          </div>

          <div className="grid gap-3 text-sm">
            {productName ? <DetailRow label="Producto" value={productName} /> : null}
            {variantName ? <DetailRow label="Variante" value={variantName} /> : null}
            <DetailRow label="Código de barras" value={row.codigo_barras} mono />
            <DetailRow label="Empresa" value={companyName} />
            <DetailRow label="Lista de precios" value={row.price_list || "—"} />
            <DetailRow label="Canal" value={row.canal} capitalize />
            {row.observacion ? <DetailRow label="Observación" value={row.observacion} /> : null}
          </div>

          <div className="rounded-xl border bg-muted/20 p-4 space-y-3">
            <div>
              <Label className="text-muted-foreground text-xs">ANTES (regular_price congelado)</Label>
              <p className="text-lg line-through">{formatCurrency(row.regular_price)}</p>
            </div>
            <div>
              <Label className="text-muted-foreground text-xs">AHORA (sale_price)</Label>
              <p className="text-3xl font-bold text-emerald-700">
                {formatCurrency(row.sale_price)}
              </p>
            </div>
            <div className="grid grid-cols-2 gap-3 border-t pt-3">
              <div>
                <Label className="text-muted-foreground text-xs">Descuento</Label>
                <p className="text-lg font-semibold">{discount}</p>
              </div>
              <div>
                <Label className="text-muted-foreground text-xs">Ahorro</Label>
                <p className="text-lg font-semibold">{formatCurrency(savings)}</p>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <Label className="text-muted-foreground text-xs">Inicio</Label>
              <p>{formatDateShort(row.fecha_inicio)}</p>
            </div>
            <div>
              <Label className="text-muted-foreground text-xs">Término</Label>
              <p>{formatDateShort(row.fecha_fin)}</p>
            </div>
          </div>
          <p className="text-muted-foreground text-xs">
            Vigencia: {formatVigencia(row.fecha_inicio, row.fecha_fin)}
          </p>

          <div className="grid gap-2">
            <Button
              className="w-full"
              onClick={() => {
                onOpenChange(false)
                onEditSalePrice(row)
              }}
            >
              <Pencil className="mr-2 h-4 w-4" />
              Editar precio AHORA
            </Button>
            <Button
              variant="outline"
              className="w-full"
              onClick={() => {
                onOpenChange(false)
                onLabels(row)
              }}
            >
              <Printer className="mr-2 h-4 w-4" />
              Generar etiquetas
            </Button>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}

function DetailRow({
  label,
  value,
  mono,
  capitalize,
}: {
  label: string
  value: string
  mono?: boolean
  capitalize?: boolean
}) {
  return (
    <div>
      <Label className="text-muted-foreground text-xs">{label}</Label>
      <p className={mono ? "font-mono text-xs" : capitalize ? "capitalize" : ""}>{value}</p>
    </div>
  )
}
