"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Loader2, Ship } from "lucide-react"

import {
  getDistribuidoraPlanificacionOperationalCosts,
  putDistribuidoraPlanificacionOperationalCosts,
} from "@/lib/api"
import {
  computeOperationalCostClp,
  EMPTY_OPERATIONAL_COSTS,
  type RouteOperationalCosts,
} from "@/lib/planificacion-operational-costs"
import { formatClp } from "@/lib/ors-map-ui"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

type OrsOperationalCostsPanelProps = {
  planSessionId: string | null
  truckId: number | null
  fuelCostClp: number
  loading?: boolean
  onCostsChange?: (costs: RouteOperationalCosts) => void
}

function parseClpInput(raw: string): number {
  const n = Number(raw.replace(/\s/g, "").replace(/\./g, ""))
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : 0
}

export function OrsOperationalCostsPanel({
  planSessionId,
  truckId,
  fuelCostClp,
  loading,
  onCostsChange,
}: OrsOperationalCostsPanelProps) {
  const [costs, setCosts] = useState<RouteOperationalCosts>(EMPTY_OPERATIONAL_COSTS)
  const [fetching, setFetching] = useState(false)
  const [saving, setSaving] = useState(false)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const costsRef = useRef(costs)
  costsRef.current = costs

  const applyCosts = useCallback(
    (next: RouteOperationalCosts) => {
      setCosts(next)
      onCostsChange?.(next)
    },
    [onCostsChange],
  )

  useEffect(() => {
    if (!planSessionId || !truckId) {
      applyCosts(EMPTY_OPERATIONAL_COSTS)
      return
    }
    const ac = new AbortController()
    setFetching(true)
    ;(async () => {
      try {
        const row = await getDistribuidoraPlanificacionOperationalCosts({
          planSessionId,
          truckId,
          signal: ac.signal,
        })
        if (ac.signal.aborted) return
        applyCosts({
          ferry_clp: row.ferry_clp ?? 0,
          per_diem_clp: row.per_diem_clp ?? 0,
          other_clp: row.other_clp ?? 0,
        })
      } catch {
        if (!ac.signal.aborted) applyCosts(EMPTY_OPERATIONAL_COSTS)
      } finally {
        if (!ac.signal.aborted) setFetching(false)
      }
    })()
    return () => {
      ac.abort()
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [planSessionId, truckId, applyCosts])

  const scheduleSave = useCallback(
    (next: RouteOperationalCosts) => {
      if (!planSessionId || !truckId) return
      if (debounceRef.current) clearTimeout(debounceRef.current)
      debounceRef.current = setTimeout(() => {
        setSaving(true)
        void putDistribuidoraPlanificacionOperationalCosts({
          plan_session_id: planSessionId,
          truck_id: truckId,
          ferry_clp: next.ferry_clp,
          per_diem_clp: next.per_diem_clp,
          other_clp: next.other_clp,
        })
          .catch(() => {
            /* silencioso — usuario puede reintentar editando */
          })
          .finally(() => setSaving(false))
      }, 500)
    },
    [planSessionId, truckId],
  )

  const updateField = (field: keyof RouteOperationalCosts, raw: string) => {
    const next = { ...costsRef.current, [field]: parseClpInput(raw) }
    applyCosts(next)
    scheduleSave(next)
  }

  const operationalTotal = computeOperationalCostClp(fuelCostClp, costs)
  const busy = loading || fetching

  return (
    <div className="border-b border-border/80 px-3 py-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          <Ship className="size-3.5" aria-hidden />
          Costos operacionales
        </div>
        {saving ? (
          <Loader2 className="size-3.5 animate-spin text-muted-foreground" aria-hidden />
        ) : null}
      </div>
      <dl className="space-y-2 text-xs">
        <div className="flex items-center justify-between gap-2">
          <dt className="text-muted-foreground">Combustible</dt>
          <dd className="font-medium tabular-nums text-foreground">
            {busy ? "—" : formatClp(fuelCostClp)}
          </dd>
        </div>
        <div className="grid grid-cols-[1fr_auto] items-center gap-x-2 gap-y-1">
          <Label htmlFor="ors-ferry-clp" className="text-muted-foreground">
            Ferry
          </Label>
          <Input
            id="ors-ferry-clp"
            type="number"
            min={0}
            step={1000}
            disabled={busy || !planSessionId || !truckId}
            className="h-7 w-28 justify-self-end text-xs tabular-nums"
            value={costs.ferry_clp || ""}
            onChange={(e) => updateField("ferry_clp", e.target.value)}
          />
          <Label htmlFor="ors-viaticos-clp" className="text-muted-foreground">
            Viáticos
          </Label>
          <Input
            id="ors-viaticos-clp"
            type="number"
            min={0}
            step={1000}
            disabled={busy || !planSessionId || !truckId}
            className="h-7 w-28 justify-self-end text-xs tabular-nums"
            value={costs.per_diem_clp || ""}
            onChange={(e) => updateField("per_diem_clp", e.target.value)}
          />
          <Label htmlFor="ors-otros-clp" className="text-muted-foreground">
            Otros gastos
          </Label>
          <Input
            id="ors-otros-clp"
            type="number"
            min={0}
            step={1000}
            disabled={busy || !planSessionId || !truckId}
            className="h-7 w-28 justify-self-end text-xs tabular-nums"
            value={costs.other_clp || ""}
            onChange={(e) => updateField("other_clp", e.target.value)}
          />
        </div>
        <div className="flex items-center justify-between gap-2 border-t border-border/60 pt-2">
          <dt className="font-semibold text-foreground">Costo operacional</dt>
          <dd className="font-semibold tabular-nums text-foreground">
            {busy ? "—" : formatClp(operationalTotal)}
          </dd>
        </div>
      </dl>
    </div>
  )
}
