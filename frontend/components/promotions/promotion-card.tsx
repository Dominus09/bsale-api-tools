"use client"

import Image from "next/image"
import { Copy, Pencil } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import type { PromotionGridRow } from "@/lib/api"
import {
  formatCurrency,
  formatDiscountBadge,
  formatVigencia,
  productTitleLines,
} from "@/lib/promotions-utils"
import {
  PromotionDiscountBadge,
  PromotionStatusBadge,
  PromotionTipoBadge,
} from "@/components/promotions/promotion-badges"

type PromotionCardProps = {
  row: PromotionGridRow
  companyName: string
  onOpen: (row: PromotionGridRow) => void
  onEdit: (row: PromotionGridRow) => void
  onDuplicate: (row: PromotionGridRow) => void
}

export function PromotionCard({
  row,
  companyName,
  onOpen,
  onEdit,
  onDuplicate,
}: PromotionCardProps) {
  const { line1, line2 } = productTitleLines(row)
  const discount = formatDiscountBadge(row.regular_price, row.sale_price)
  const imageUrl = row.image_url?.trim() || null

  return (
    <Card
      className="group cursor-pointer overflow-hidden border shadow-sm transition-shadow hover:shadow-md"
      onClick={() => onOpen(row)}
    >
      <CardContent className="flex h-full flex-col p-0">
        <div className="flex items-start justify-between gap-2 border-b bg-muted/30 px-3 py-2">
          <PromotionStatusBadge estado={row.estado} />
          <PromotionTipoBadge tipo={row.tipo} />
        </div>

        <div className="relative flex aspect-[4/3] items-center justify-center bg-white p-4">
          {imageUrl ? (
            <Image
              src={imageUrl}
              alt={line1}
              fill
              className="object-contain p-3"
              sizes="(max-width: 640px) 50vw, 20vw"
              unoptimized
            />
          ) : (
            <div className="text-muted-foreground flex h-full w-full items-center justify-center rounded-lg bg-muted/40 text-xs">
              Sin imagen
            </div>
          )}
        </div>

        <div className="flex flex-1 flex-col gap-3 p-4">
          <div className="min-h-[2.5rem]">
            <p className="text-sm font-bold leading-tight tracking-tight">{line1}</p>
            {line2 ? (
              <p className="text-muted-foreground text-xs font-semibold leading-tight">{line2}</p>
            ) : null}
          </div>

          <div className="space-y-1">
            <p className="text-muted-foreground text-sm line-through">
              ANTES {formatCurrency(row.regular_price)}
            </p>
            <p className="text-2xl font-bold text-emerald-700">
              AHORA {formatCurrency(row.sale_price)}
            </p>
          </div>

          <PromotionDiscountBadge label={discount} />

          <div className="text-muted-foreground mt-auto space-y-1 text-xs">
            <p>
              <span className="text-foreground/70 font-medium">Empresa:</span> {companyName}
            </p>
            <p>
              <span className="text-foreground/70 font-medium">Vigencia:</span>{" "}
              {formatVigencia(row.fecha_inicio, row.fecha_fin)}
            </p>
          </div>

          <div
            className="flex gap-2 pt-1"
            onClick={(e) => e.stopPropagation()}
          >
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="flex-1"
              onClick={() => onEdit(row)}
            >
              <Pencil className="mr-1.5 h-3.5 w-3.5" />
              Editar
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="flex-1"
              onClick={() => onDuplicate(row)}
            >
              <Copy className="mr-1.5 h-3.5 w-3.5" />
              Duplicar
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
