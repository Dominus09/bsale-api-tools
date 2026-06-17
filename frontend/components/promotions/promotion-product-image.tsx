"use client"

import { useCallback, useState } from "react"
import { Package } from "lucide-react"
import { cn } from "@/lib/utils"

export const PROMOTION_LOCAL_IMAGE_BASE = "/imagenes-productos"

export function localBarcodeImageUrl(barcode: string | null | undefined): string | null {
  const bc = (barcode || "").trim()
  if (!bc) return null
  return `${PROMOTION_LOCAL_IMAGE_BASE}/${bc}.webp`
}

type ImageStage = "remote" | "local" | "placeholder"

function initialStage(imageUrl?: string | null, barcode?: string | null): ImageStage {
  if (imageUrl?.trim()) return "remote"
  if (localBarcodeImageUrl(barcode)) return "local"
  return "placeholder"
}

type PromotionProductImageProps = {
  imageUrl?: string | null
  barcode?: string | null
  alt?: string
  className?: string
  imgClassName?: string
}

export function PromotionProductImage({
  imageUrl,
  barcode,
  alt = "Producto",
  className,
  imgClassName,
}: PromotionProductImageProps) {
  const remote = imageUrl?.trim() || null
  const local = localBarcodeImageUrl(barcode)

  const [stage, setStage] = useState<ImageStage>(() => initialStage(remote, barcode))

  const handleError = useCallback(() => {
    setStage((prev) => {
      if (prev === "remote" && local) return "local"
      return "placeholder"
    })
  }, [local])

  if (stage === "placeholder") {
    return (
      <div
        className={cn(
          "flex h-full w-full flex-col items-center justify-center gap-1 bg-muted/30 text-muted-foreground",
          className,
        )}
      >
        <Package className="h-8 w-8 opacity-40" aria-hidden />
        <span className="text-[10px] font-medium opacity-60">Sin imagen</span>
      </div>
    )
  }

  const src = stage === "remote" ? remote : local
  if (!src) {
    return (
      <div className={cn("flex h-full w-full items-center justify-center bg-muted/30", className)}>
        <Package className="h-8 w-8 text-muted-foreground opacity-40" aria-hidden />
      </div>
    )
  }

  return (
    <div className={cn("relative h-full w-full", className)}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        src={src}
        alt={alt}
        className={cn("absolute inset-0 h-full w-full object-contain p-2", imgClassName)}
        onError={handleError}
      />
    </div>
  )
}
