"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { getPurchaseDataFreshness, type PurchaseDataFreshness } from "@/lib/api"

type Props = { companyId: number | null }

function stockCardClass(status: PurchaseDataFreshness["stock"]["status"]): string {
  if (status === "OK") return "border-emerald-200/90 bg-emerald-50/70"
  if (status === "REVISAR") return "border-amber-200/90 bg-amber-50/70"
  return "border-red-200/90 bg-red-50/70"
}

function salesCardClass(status: PurchaseDataFreshness["sales"]["status"]): string {
  if (status === "OK") return "border-emerald-200/90 bg-emerald-50/70"
  if (status === "ESPERANDO ACTUALIZACIÓN") return "border-amber-200/90 bg-amber-50/70"
  return "border-red-200/90 bg-red-50/70"
}

const REFRESH_MS = 90_000

/** Indicador informativo de frescura de stock (~20 min) y ventas (corte diario 05:00 Chile). */
export function ComprasDataStatusCard({ companyId }: Props) {
  const [data, setData] = useState<PurchaseDataFreshness | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState("")

  useEffect(() => {
    if (companyId == null || !Number.isFinite(companyId)) {
      setData(null)
      setErr("")
      setLoading(false)
      return
    }

    let cancelled = false

    const load = (showSpinner: boolean) => {
      if (showSpinner) {
        setLoading(true)
        setErr("")
      }
      void getPurchaseDataFreshness(companyId)
        .then((d) => {
          if (!cancelled) {
            setData(d)
            setErr("")
          }
        })
        .catch((e) => {
          if (!cancelled) {
            setData(null)
            setErr(e instanceof Error ? e.message : "Error al cargar")
          }
        })
        .finally(() => {
          if (!cancelled && showSpinner) setLoading(false)
        })
    }

    load(true)
    const timer = window.setInterval(() => load(false), REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [companyId])

  if (companyId == null || !Number.isFinite(companyId)) {
    return (
      <div className="rounded-xl border border-border/80 bg-muted/40/60 px-4 py-3 text-sm text-muted-foreground">
        <p className="font-semibold text-foreground">Estado de datos</p>
        <p className="mt-1 text-muted-foreground">Elige una empresa para ver la frescura de stock y ventas.</p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-border/80 bg-card p-4 shadow-sm">
      <div className="mb-3 flex items-center gap-2">
        <h2 className="text-sm font-semibold tracking-tight text-foreground">Estado de datos</h2>
        {loading && !data ? <Loader2 className="size-4 animate-spin text-muted-foreground" aria-hidden /> : null}
      </div>
      {err ? <p className="text-sm text-red-700 dark:text-red-300">{err}</p> : null}
      {data ? (
        <div className="grid gap-3 sm:grid-cols-2">
          <div
            className={`rounded-lg border px-3 py-2.5 ${stockCardClass(data.stock.status)}`}
          >
            <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Stock</p>
            <p className="mt-1 text-sm text-foreground">{data.stock.message}</p>
            <p className="mt-1 text-xs text-muted-foreground">Sincronización frecuente; umbrales 30 / 60 min.</p>
          </div>
          <div
            className={`rounded-lg border px-3 py-2.5 ${salesCardClass(data.sales.status)}`}
          >
            <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Ventas</p>
            <p className="mt-1 text-sm text-foreground">{data.sales.message}</p>
            <p className="mt-1 text-xs text-muted-foreground">Regla diaria con corte 05:00 (hora Chile).</p>
          </div>
        </div>
      ) : !err ? (
        <p className="text-sm text-muted-foreground">Cargando…</p>
      ) : null}
    </div>
  )
}
