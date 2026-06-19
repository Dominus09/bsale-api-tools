"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Fuel, Loader2, Ship } from "lucide-react"

import {
  getDistribuidoraPlanificacionFuelConfig,
  getDistribuidoraPlanificacionOperationalCosts,
  putDistribuidoraPlanificacionFuelConfig,
  putDistribuidoraPlanificacionOperationalCosts,
} from "@/lib/api"
import {
  computeOperationalCostClp,
  DEFAULT_DIESEL_CLP_PER_LITER,
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
  onDieselChange?: (dieselClpPerLiter: number) => void
}

function parseClpInput(raw: string): number {
  const n = Number(raw.replace(/\s/g, "").replace(/\./g, ""))
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : 0
}

function parseDieselInput(raw: string): number {
  const n = Number(raw.replace(/\s/g, ""))
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_DIESEL_CLP_PER_LITER
}

export function OrsOperationalCostsPanel({
  planSessionId,
  truckId,
  fuelCostClp,
  loading,
  onCostsChange,
  onDieselChange,
}: OrsOperationalCostsPanelProps) {
  const [costs, setCosts] = useState<RouteOperationalCosts>(EMPTY_OPERATIONAL_COSTS)
  const [dieselInput, setDieselInput] = useState(String(DEFAULT_DIESEL_CLP_PER_LITER))
  const [fetching, setFetching] = useState(false)
  const [saving, setSaving] = useState(false)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const dieselDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
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
    const ac = new AbortController()
    setFetching(true)
    ;(async () => {
      try {
        const fuelCfg = await getDistribuidoraPlanificacionFuelConfig({ signal: ac.signal })
        let next = {
          ...EMPTY_OPERATIONAL_COSTS,
          diesel_clp_per_liter: fuelCfg.diesel_price_per_liter || DEFAULT_DIESEL_CLP_PER_LITER,
        }
        if (planSessionId && truckId) {
          const row = await getDistribuidoraPlanificacionOperationalCosts({
            planSessionId,
            truckId,
            signal: ac.signal,
          })
          next = {
            ferry_clp: row.ferry_clp ?? 0,
            per_diem_clp: row.per_diem_clp ?? 0,
            other_clp: row.other_clp ?? 0,
            diesel_clp_per_liter:
              row.diesel_clp_per_liter ??
              fuelCfg.diesel_price_per_liter ??
              DEFAULT_DIESEL_CLP_PER_LITER,
          }
        }
        if (ac.signal.aborted) return
        applyCosts(next)
        setDieselInput(String(Math.round(next.diesel_clp_per_liter)))
      } catch {
        if (!ac.signal.aborted) {
          const fallback = { ...EMPTY_OPERATIONAL_COSTS }
          applyCosts(fallback)
          setDieselInput(String(DEFAULT_DIESEL_CLP_PER_LITER))
        }
      } finally {
        if (!ac.signal.aborted) setFetching(false)
      }
    })()
    return () => {
      ac.abort()
      if (debounceRef.current) clearTimeout(debounceRef.current)
      if (dieselDebounceRef.current) clearTimeout(dieselDebounceRef.current)
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
          diesel_clp_per_liter: next.diesel_clp_per_liter,
        })
          .catch(() => {
            /* silencioso */
          })
          .finally(() => setSaving(false))
      }, 500)
    },
    [planSessionId, truckId],
  )

  const scheduleDieselSave = useCallback(
    (diesel: number) => {
      if (dieselDebounceRef.current) clearTimeout(dieselDebounceRef.current)
      dieselDebounceRef.current = setTimeout(() => {
        setSaving(true)
        void putDistribuidoraPlanificacionFuelConfig(diesel)
          .then(() => {
            onDieselChange?.(diesel)
            if (planSessionId && truckId) {
              return putDistribuidoraPlanificacionOperationalCosts({
                plan_session_id: planSessionId,
                truck_id: truckId,
                ferry_clp: costsRef.current.ferry_clp,
                per_diem_clp: costsRef.current.per_diem_clp,
                other_clp: costsRef.current.other_clp,
                diesel_clp_per_liter: diesel,
              })
            }
          })
          .catch(() => {
            /* silencioso */
          })
          .finally(() => setSaving(false))
      }, 600)
    },
    [onDieselChange, planSessionId, truckId],
  )

  const updateField = (field: "ferry_clp" | "per_diem_clp" | "other_clp", raw: string) => {
    const next = { ...costsRef.current, [field]: parseClpInput(raw) }
    applyCosts(next)
    scheduleSave(next)
  }

  const updateDiesel = (raw: string) => {
    setDieselInput(raw)
    const diesel = parseDieselInput(raw)
    const next = { ...costsRef.current, diesel_clp_per_liter: diesel }
    applyCosts(next)
    scheduleDieselSave(diesel)
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
        <div className="grid grid-cols-[1fr_auto] items-center gap-x-2 gap-y-1">
          <Label htmlFor="ors-diesel-panel" className="inline-flex items-center gap-1 text-muted-foreground">
            <Fuel className="size-3" aria-hidden />
            Diesel CLP/L
          </Label>
          <Input
            id="ors-diesel-panel"
            type="number"
            min={1}
            step={1}
            disabled={busy}
            className="h-7 w-28 justify-self-end text-xs tabular-nums"
            value={dieselInput}
            onChange={(e) => updateDiesel(e.target.value)}
          />
        </div>
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
