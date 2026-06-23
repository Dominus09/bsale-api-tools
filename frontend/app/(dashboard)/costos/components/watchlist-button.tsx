"use client"

import { useCallback, useEffect, useState } from "react"
import { Loader2, Star } from "lucide-react"

import {
  addCostWatchlistItem,
  getCostWatchlistStatus,
  removeCostWatchlistItem,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

export function WatchlistButton({
  companyId,
  variantId,
  size = "default",
  className,
  onChange,
}: {
  companyId: number
  variantId: number
  size?: "default" | "sm"
  className?: string
  onChange?: (onList: boolean) => void
}) {
  const [onList, setOnList] = useState(false)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)

  const refresh = useCallback(async () => {
    setLoading(true)
    try {
      const st = await getCostWatchlistStatus(companyId, variantId)
      setOnList(Boolean(st.on_watchlist))
      onChange?.(Boolean(st.on_watchlist))
    } catch {
      setOnList(false)
    } finally {
      setLoading(false)
    }
  }, [companyId, variantId, onChange])

  useEffect(() => {
    void refresh()
  }, [refresh])

  const toggle = async () => {
    setBusy(true)
    try {
      if (onList) {
        await removeCostWatchlistItem(companyId, variantId)
        setOnList(false)
        onChange?.(false)
      } else {
        await addCostWatchlistItem(companyId, variantId)
        setOnList(true)
        onChange?.(true)
      }
    } finally {
      setBusy(false)
    }
  }

  if (loading) {
    return <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
  }

  return (
    <Button
      type="button"
      size={size}
      variant={onList ? "default" : "outline"}
      className={cn(className)}
      disabled={busy}
      onClick={() => void toggle()}
    >
      <Star className={cn("mr-1 h-4 w-4", onList && "fill-current")} />
      {onList ? "En Watchlist" : "Agregar a Watchlist"}
    </Button>
  )
}
