"use client"

import Image from "next/image"
import { Pencil } from "lucide-react"
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

type PromotionDetailSheetProps = {
  row: PromotionGridRow | null
  open: boolean
  companyName: string
  onOpenChange: (open: boolean) => void
  onEditSalePrice: (row: PromotionGridRow) => void
}

export function PromotionDetailSheet({
  row,
  open,
  companyName,
  onOpenChange,
  onEditSalePrice,
}: PromotionDetailSheetProps) {
  if (!row) return null

  const discount =
    calcDiscountPercent(row.regular_price, row.sale_price) != null
      ? `-${calcDiscountPercent(row.regular_price, row.sale_price)}%`
      : "—"
  const imageUrl = row.image_url?.trim() || null

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle className="text-left">{productDisplayName(row)}</SheetTitle>
          <SheetDescription className="text-left">
            Detalle de promoción · snapshot congelado
          </SheetDescription>
        </SheetHeader>

        <div className="mt-6 space-y-6">
          <div className="flex flex-wrap items-center gap-2">
            <PromotionStatusBadge estado={row.estado} />
            <PromotionTipoBadge tipo={row.tipo} />
            <PromotionDiscountBadge label={discount} />
          </div>

          <div className="relative mx-auto aspect-square w-full max-w-[220px] rounded-xl border bg-white p-4">
            {imageUrl ? (
              <Image
                src={imageUrl}
                alt={productDisplayName(row)}
                fill
                className="object-contain p-2"
                sizes="220px"
                unoptimized
              />
            ) : (
              <div className="text-muted-foreground flex h-full items-center justify-center text-sm">
                Sin imagen
              </div>
            )}
          </div>

          <div className="grid gap-4 text-sm">
            <DetailRow label="Empresa" value={companyName} />
            <DetailRow label="Lista de precio" value={row.price_list || "—"} mono />
            <DetailRow label="Código de barras" value={row.codigo_barras} mono />
            <DetailRow label="Canal" value={row.canal} capitalize />
            {row.observacion ? (
              <DetailRow label="Observación" value={row.observacion} />
            ) : null}
            {row.tipo_producto ? (
              <DetailRow label="Tipo producto" value={row.tipo_producto} />
            ) : null}
          </div>

          <div className="rounded-xl border bg-muted/30 p-4 space-y-3">
            <div>
              <Label className="text-muted-foreground text-xs">Precio original congelado (ANTES)</Label>
              <p className="text-lg line-through">{formatCurrency(row.regular_price)}</p>
            </div>
            <div>
              <Label className="text-muted-foreground text-xs">Precio promoción (AHORA)</Label>
              <p className="text-3xl font-bold text-emerald-700">
                {formatCurrency(row.sale_price)}
              </p>
            </div>
            <div>
              <Label className="text-muted-foreground text-xs">Descuento</Label>
              <p className="text-lg font-semibold">{discount}</p>
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
