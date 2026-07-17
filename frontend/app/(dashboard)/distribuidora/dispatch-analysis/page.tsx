"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, RefreshCw, Truck } from "lucide-react"
import { useRouter } from "next/navigation"

import {
  getDistribuidoraOrdersPurchase,
  type DistribuidoraPurchaseOrder,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { cn } from "@/lib/utils"

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function communeKey(row: DistribuidoraPurchaseOrder): string {
  const t = row.municipality?.trim() || row.city?.trim()
  return t || "__NONE__"
}

function sellerLabel(row: DistribuidoraPurchaseOrder): string {
  return (
    row.seller_name?.trim() ||
    row.seller?.trim() ||
    "(Sin vendedor)"
  )
}

/** Clave estable para filtrar/agrupar vendedor (user_id si existe; si no, por etiqueta). */
function sellerKey(row: DistribuidoraPurchaseOrder): string {
  const id = row.user_id
  if (id != null && Number.isFinite(Number(id))) return `u:${Number(id)}`
  return `anon:${sellerLabel(row)}`
}

const WEEKDAY_TOKENS = [
  "Lunes",
  "Martes",
  "Miércoles",
  "Jueves",
  "Viernes",
  "Sábado",
  "Domingo",
] as const

function observacionesMatchesAnyDay(
  observaciones: string | null | undefined,
  days: Set<string>,
): boolean {
  if (days.size === 0) return true
  const obs = (observaciones ?? "").toLowerCase()
  if (!obs.trim()) return false
  for (const d of days) {
    if (obs.includes(d.toLowerCase())) return true
  }
  return false
}

type MuniOption = { value: string; label: string }
type SellerOption = { key: string; label: string }

type LoadTier = "baja" | "media" | "alta"

/** Terciles por monto dentro del conjunto mostrado (sin reglas arbitrarias fijas). */
function loadTierByAmountRank(
  rows: { key: string; totalAmount: number }[],
): Map<string, LoadTier> {
  const map = new Map<string, LoadTier>()
  if (rows.length === 0) return map
  const sorted = [...rows].sort((a, b) => b.totalAmount - a.totalAmount)
  const n = sorted.length
  const third = Math.max(1, Math.ceil(n / 3))
  sorted.forEach((r, i) => {
    if (i < third) map.set(r.key, "alta")
    else if (i < 2 * third) map.set(r.key, "media")
    else map.set(r.key, "baja")
  })
  return map
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

export default function DispatchAnalysisPage() {
  const router = useRouter()
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(false)
  const [selectedDays, setSelectedDays] = useState<Set<string>>(() => new Set())
  const [selectedMunicipalities, setSelectedMunicipalities] = useState<Set<string>>(
    () => new Set(),
  )
  const [selectedSellers, setSelectedSellers] = useState<Set<string>>(() => new Set())

  const [items, setItems] = useState<DistribuidoraPurchaseOrder[]>([])
  const [hasMore, setHasMore] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraOrdersPurchase({
        emission_date_from: dateFrom,
        emission_date_to: dateTo,
        only_not_invoiced: onlyNotInvoiced,
        limit: 500,
        offset: 0,
      })
      setItems(res.items)
      setHasMore(res.has_more)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar datos")
      setItems([])
      setHasMore(false)
    } finally {
      setLoading(false)
    }
  }, [dateFrom, dateTo, onlyNotInvoiced])

  useEffect(() => {
    void load()
  }, [load])

  const municipalityOptions = useMemo((): MuniOption[] => {
    const seen = new Map<string, string>()
    for (const r of items) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      seen.set(k, label)
    }
    return Array.from(seen.entries())
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [items])

  const sellerOptions = useMemo((): SellerOption[] => {
    const seen = new Map<string, string>()
    for (const r of items) {
      const k = sellerKey(r)
      if (!seen.has(k)) seen.set(k, sellerLabel(r))
    }
    return Array.from(seen.entries())
      .map(([key, label]) => ({ key, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [items])

  const filtered = useMemo(() => {
    return items.filter((r) => {
      if (!observacionesMatchesAnyDay(r.observaciones, selectedDays)) return false
      if (selectedMunicipalities.size > 0 && !selectedMunicipalities.has(communeKey(r)))
        return false
      if (selectedSellers.size > 0 && !selectedSellers.has(sellerKey(r))) return false
      return true
    })
  }, [items, selectedDays, selectedMunicipalities, selectedSellers])

  const summaryByMunicipality = useMemo(() => {
    const m = new Map<
      string,
      { label: string; clients: Set<number>; amount: number }
    >()
    for (const r of filtered) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      if (!m.has(k)) m.set(k, { label, clients: new Set(), amount: 0 })
      const row = m.get(k)!
      const cid = r.client_id
      if (cid != null && Number.isFinite(Number(cid))) row.clients.add(Number(cid))
      row.amount += Number(r.total_amount ?? 0)
    }
    return Array.from(m.entries())
      .map(([key, v]) => ({
        key,
        label: v.label,
        clientCount: v.clients.size,
        totalAmount: v.amount,
      }))
      .sort((a, b) => b.totalAmount - a.totalAmount)
  }, [filtered])

  const cityLoadTiers = useMemo(
    () => loadTierByAmountRank(summaryByMunicipality),
    [summaryByMunicipality],
  )

  const summaryBySeller = useMemo(() => {
    const m = new Map<string, { clients: Set<number>; amount: number }>()
    for (const r of filtered) {
      const k = sellerLabel(r)
      if (!m.has(k)) m.set(k, { clients: new Set(), amount: 0 })
      const row = m.get(k)!
      const cid = r.client_id
      if (cid != null && Number.isFinite(Number(cid))) row.clients.add(Number(cid))
      row.amount += Number(r.total_amount ?? 0)
    }
    return Array.from(m.entries())
      .map(([seller, v]) => ({
        seller,
        clientCount: v.clients.size,
        totalAmount: v.amount,
      }))
      .sort((a, b) => b.totalAmount - a.totalAmount)
  }, [filtered])

  const toggleDay = (d: string) => {
    setSelectedDays((prev) => {
      const n = new Set(prev)
      if (n.has(d)) n.delete(d)
      else n.add(d)
      return n
    })
  }

  const toggleMunicipality = (value: string) => {
    setSelectedMunicipalities((prev) => {
      const n = new Set(prev)
      if (n.has(value)) n.delete(value)
      else n.add(value)
      return n
    })
  }

  const toggleSeller = (key: string) => {
    setSelectedSellers((prev) => {
      const n = new Set(prev)
      if (n.has(key)) n.delete(key)
      else n.add(key)
      return n
    })
  }

  const goToOrders = () => {
    const qs = new URLSearchParams()
    qs.set("emission_date_from", dateFrom)
    qs.set("emission_date_to", dateTo)
    if (onlyNotInvoiced) qs.set("only_not_invoiced", "true")
    if (selectedDays.size > 0) {
      qs.set("delivery_search", Array.from(selectedDays).join(","))
    }
    if (selectedMunicipalities.size > 0) {
      qs.set("municipality", Array.from(selectedMunicipalities).join(","))
    }
    if (selectedSellers.size === 1) {
      const only = Array.from(selectedSellers)[0]
      if (only.startsWith("u:")) {
        const id = parseInt(only.slice(2), 10)
        if (Number.isFinite(id)) qs.set("user_id", String(id))
      }
    }
    router.push(`/distribuidora/orders?${qs.toString()}`)
  }

  const truncated = hasMore

  const loadBadgeClass = (tier: LoadTier) =>
    cn(
      "font-normal",
      tier === "baja" &&
        "border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200",
      tier === "media" &&
        "border-amber-500/40 bg-amber-500/10 text-amber-900 dark:text-amber-100",
      tier === "alta" &&
        "border-orange-600/40 bg-orange-500/10 text-orange-950 dark:text-orange-100",
    )

  const loadBadgeLabel = (tier: LoadTier) =>
    tier === "baja" ? "Carga baja" : tier === "media" ? "Carga media" : "Carga alta"

  return (
    <div className="mx-auto flex max-w-[1280px] flex-col gap-8 pb-10">
      <header className="flex flex-col gap-3 border-b pb-6 sm:flex-row sm:items-end sm:justify-between">
        <div className="space-y-1">
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora
          </p>
          <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
            Análisis previo a despacho
          </h1>
          <p className="max-w-xl text-sm text-muted-foreground">
            Con los días de entrega elegidos, ve carga por ciudad y vendedor. Luego pasa
            a planificar rutas con un clic.
          </p>
        </div>
        <div className="flex shrink-0 flex-col items-stretch gap-2 sm:items-end">
          <p className="text-right text-xs text-muted-foreground">
            {loading ? (
              <span className="inline-flex items-center gap-1.5">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Actualizando…
              </span>
            ) : (
              <>
                <span className="font-medium text-foreground">{filtered.length}</span>{" "}
                pedidos en vista
                {items.length ? (
                  <>
                    {" "}
                    · {items.length} cargados
                  </>
                ) : null}
              </>
            )}
          </p>
          <Button
            type="button"
            size="lg"
            className="gap-2 shadow-sm"
            onClick={goToOrders}
          >
            <Truck className="h-4 w-4" />
            Ver pedidos para planificar
          </Button>
        </div>
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {truncated ? (
        <Alert>
          <AlertTitle>Aviso</AlertTitle>
          <AlertDescription>
            Se cargaron las primeras {items.length} órdenes del rango (límite 500) y
            hay más resultados. Los totales pueden quedar incompletos.
          </AlertDescription>
        </Alert>
      ) : null}

      {/* Filtros */}
      <section className="rounded-xl border bg-card p-5 shadow-sm">
        <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-sm font-semibold text-foreground">Filtros</h2>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1.5"
            onClick={() => void load()}
            disabled={loading}
          >
            <RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />
            Actualizar datos
          </Button>
        </div>

        <div className="flex flex-col gap-8">
          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-4 lg:items-end">
            <div className="space-y-2">
              <Label htmlFor="da-from" className="text-xs uppercase text-muted-foreground">
                Emisión desde
              </Label>
              <Input
                id="da-from"
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                disabled={loading}
                className="bg-background"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="da-to" className="text-xs uppercase text-muted-foreground">
                Emisión hasta
              </Label>
              <Input
                id="da-to"
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                disabled={loading}
                className="bg-background"
              />
            </div>
            <div className="flex items-center gap-2 rounded-lg border bg-muted/30 px-3 py-2.5 lg:col-span-2">
              <Checkbox
                id="da-notinv"
                checked={onlyNotInvoiced}
                onCheckedChange={(c) => setOnlyNotInvoiced(c === true)}
                disabled={loading}
              />
              <label htmlFor="da-notinv" className="cursor-pointer text-sm">
                Solo pedidos sin factura/boleta enlazada (document_related)
              </label>
            </div>
          </div>

          <div className="space-y-3">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <Label className="text-xs font-semibold uppercase tracking-wide text-foreground">
                Días de entrega
              </Label>
              <span className="text-xs text-muted-foreground">
                Principal · varios = OR · vacío = todos los días
              </span>
            </div>
            <div className="flex flex-wrap gap-2">
              {WEEKDAY_TOKENS.map((d) => {
                const on = selectedDays.has(d)
                return (
                  <button
                    key={d}
                    type="button"
                    onClick={() => toggleDay(d)}
                    disabled={loading}
                    className={cn(
                      "rounded-full border px-4 py-2 text-sm font-medium transition-colors",
                      "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      on
                        ? "border-primary bg-primary text-primary-foreground shadow-sm"
                        : "border-border bg-background text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                      loading && "pointer-events-none opacity-60",
                    )}
                  >
                    {d}
                  </button>
                )
              })}
            </div>
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <Label className="text-xs font-semibold uppercase tracking-wide">
                  Ciudad / comuna
                </Label>
                {selectedMunicipalities.size > 0 ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => setSelectedMunicipalities(new Set())}
                  >
                    Limpiar
                  </Button>
                ) : null}
              </div>
              <div className="max-h-36 overflow-y-auto rounded-lg border bg-muted/20 p-3 text-sm">
                {municipalityOptions.length === 0 ? (
                  <p className="text-muted-foreground">Sin datos en el rango.</p>
                ) : (
                  <ul className="space-y-2.5">
                    {municipalityOptions.map((o) => (
                      <li key={o.value} className="flex items-center gap-2.5">
                        <Checkbox
                          id={`m-${encodeURIComponent(o.value)}`}
                          checked={selectedMunicipalities.has(o.value)}
                          onCheckedChange={() => toggleMunicipality(o.value)}
                        />
                        <label
                          htmlFor={`m-${encodeURIComponent(o.value)}`}
                          className="cursor-pointer leading-tight"
                        >
                          {o.label}
                        </label>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <Label className="text-xs font-semibold uppercase tracking-wide">
                  Vendedor
                </Label>
                {selectedSellers.size > 0 ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => setSelectedSellers(new Set())}
                  >
                    Limpiar
                  </Button>
                ) : null}
              </div>
              <div className="max-h-36 overflow-y-auto rounded-lg border bg-muted/20 p-3 text-sm">
                {sellerOptions.length === 0 ? (
                  <p className="text-muted-foreground">Sin datos en el rango.</p>
                ) : (
                  <ul className="space-y-2.5">
                    {sellerOptions.map((o) => (
                      <li key={o.key} className="flex items-center gap-2.5">
                        <Checkbox
                          id={`s-${encodeURIComponent(o.key)}`}
                          checked={selectedSellers.has(o.key)}
                          onCheckedChange={() => toggleSeller(o.key)}
                        />
                        <label
                          htmlFor={`s-${encodeURIComponent(o.key)}`}
                          className="cursor-pointer leading-tight"
                        >
                          {o.label}
                        </label>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </div>

          <div className="flex justify-center border-t pt-5">
            <Button
              type="button"
              size="lg"
              className="min-w-[260px] gap-2 shadow-md"
              onClick={goToOrders}
            >
              <Truck className="h-4 w-4" />
              Ver pedidos para planificar
            </Button>
          </div>
        </div>
      </section>

      {/* Ciudades: tarjetas */}
      <section className="space-y-4">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Carga por ciudad</h2>
          <p className="text-sm text-muted-foreground">
            Ordenado por monto. La etiqueta de carga compara ciudades entre sí en esta
            vista (terciles por monto).
          </p>
        </div>
        {summaryByMunicipality.length === 0 ? (
          <p className="rounded-lg border border-dashed py-10 text-center text-sm text-muted-foreground">
            No hay pedidos con los filtros actuales. Ajuste días, ciudad o vendedor.
          </p>
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {summaryByMunicipality.map((r) => {
              const tier = cityLoadTiers.get(r.key) ?? "media"
              return (
                <div
                  key={r.key}
                  className="flex flex-col justify-between rounded-xl border bg-card p-5 shadow-sm transition-shadow hover:shadow-md"
                >
                  <div className="flex items-start justify-between gap-2">
                    <h3 className="text-lg font-semibold leading-tight tracking-tight">
                      {r.label}
                    </h3>
                    <Badge variant="outline" className={loadBadgeClass(tier)}>
                      {loadBadgeLabel(tier)}
                    </Badge>
                  </div>
                  <p className="mt-3 text-sm text-muted-foreground">
                    <span className="text-base font-semibold text-foreground">
                      {r.clientCount}
                    </span>{" "}
                    {r.clientCount === 1 ? "cliente" : "clientes"}
                  </p>
                  <p className="mt-4 text-2xl font-semibold tabular-nums tracking-tight text-foreground sm:text-3xl">
                    {formatCLP(r.totalAmount)}
                  </p>
                </div>
              )
            })}
          </div>
        )}
      </section>

      {/* Vendedores: compacto */}
      <section className="space-y-4">
        <h2 className="text-lg font-semibold tracking-tight">Concentración por vendedor</h2>
        {summaryBySeller.length === 0 ? (
          <p className="rounded-lg border border-dashed py-8 text-center text-sm text-muted-foreground">
            Sin datos para mostrar.
          </p>
        ) : (
          <ul className="divide-y rounded-xl border bg-card shadow-sm">
            {summaryBySeller.map((r) => (
              <li
                key={r.seller}
                className="flex flex-wrap items-baseline gap-x-2 gap-y-1 px-4 py-3 text-sm sm:px-5"
              >
                <span className="font-medium text-foreground">{r.seller}</span>
                <span className="text-muted-foreground">—</span>
                <span className="text-muted-foreground">
                  {r.clientCount} {r.clientCount === 1 ? "cliente" : "clientes"}
                </span>
                <span className="text-muted-foreground">—</span>
                <span className="font-medium tabular-nums text-foreground">
                  {formatCLP(r.totalAmount)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}
