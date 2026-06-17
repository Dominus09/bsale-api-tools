"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Package } from "lucide-react"
import { cn } from "@/lib/utils"
import {
  PRODUCT_IMAGE_PLACEHOLDER,
  productImageFallbackUrls,
} from "@/lib/product-photo"

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
  const urls = useMemo(
    () => productImageFallbackUrls(imageUrl, barcode),
    [imageUrl, barcode],
  )

  const [index, setIndex] = useState(0)

  useEffect(() => {
    setIndex(0)
  }, [urls])

  const handleError = useCallback(() => {
    setIndex((prev) => prev + 1)
  }, [])

  const src = index < urls.length ? urls[index] : null

  if (!src) {
    return (
      <div
        className={cn(
          "flex h-full w-full flex-col items-center justify-center gap-1 bg-muted/30 text-muted-foreground",
          className,
        )}
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          src={PRODUCT_IMAGE_PLACEHOLDER}
          alt=""
          aria-hidden
          className="h-10 w-10 opacity-30"
        />
        <span className="text-[10px] font-medium opacity-60">Sin imagen</span>
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
