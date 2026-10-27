"use client"

import { useState } from "react"
import { Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  AnalyticsV2Error,
  createCostReview,
  startCostReview,
} from "@/lib/analytics-v2/api"
import type { CostReviewCreateBody, CostReviewItem } from "@/lib/analytics-v2/types"
import { hasManagementAccess } from "@/lib/permissions"
import { useAuthMe } from "@/lib/use-auth-me"

/**
 * Crea (o reutiliza) un caso PENDIENTE y luego inicia la revisión.
 * El responsable lo asigna el backend desde el JWT; nunca se envía actor_user_id.
 */
export function CostV2StartReviewButton({
  companyId,
  payload,
  label = "Iniciar revisión",
  size = "sm",
  variant = "default",
  onStarted,
  className,
}: {
  companyId: number
  payload: CostReviewCreateBody
  label?: string
  size?: "sm" | "default" | "lg" | "icon"
  variant?: "default" | "outline" | "secondary" | "ghost"
  onStarted?: (item: CostReviewItem) => void
  className?: string
}) {
  const { meState, staff } = useAuthMe()
  const canMutate = meState === "ok" && hasManagementAccess(staff)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  if (meState === "loading") {
    return <span className="text-[11px] text-muted-foreground">Verificando permisos…</span>
  }

  if (!canMutate) {
    return (
      <p className="text-[11px] text-muted-foreground">
        Solo gerencia/administración puede iniciar revisiones humanas.
      </p>
    )
  }

  const run = async () => {
    setBusy(true)
    setError(null)
    try {
      const created = await createCostReview(companyId, payload)
      let item = created.item
      if (item.status === "pendiente") {
        item = await startCostReview(companyId, item.id)
      }
      onStarted?.(item)
    } catch (e: unknown) {
      setError(e instanceof AnalyticsV2Error ? e.message : e instanceof Error ? e.message : "Error al iniciar")
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className={className}>
      <Button type="button" size={size} variant={variant} disabled={busy} onClick={() => void run()}>
        {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
        {label}
      </Button>
      {error ? <p className="mt-1 text-xs text-destructive">{error}</p> : null}
    </div>
  )
}
