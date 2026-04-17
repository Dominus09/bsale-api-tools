"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, RefreshCw, ShoppingBag, Timer, Users, Wallet } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  getDistribuidoraClientsConsolidated,
  getDistribuidoraClientsFrequency,
  getDistribuidoraClientsInactive,
  getDistribuidoraClientsSummarySellers,
  getDistribuidoraClientsTop,
  getDistribuidoraSales,
  type DistribuidoraClientConsolidated,
  type DistribuidoraClientFrequency,
  type DistribuidoraClientInactive,
  type DistribuidoraClientSellerSummary,
  type DistribuidoraClientTop,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const ALL_VALUE = "__todos__"

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function addDaysIso(iso: string, delta: number): string {
  const [y, mo, da] = iso.split("-").map(Number)
  const dt = new Date(y, mo - 1, da)
  dt.setDate(dt.getDate() + delta)
  return localIsoDate(dt)
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString("es-CL", {
    dateStyle: "short",
    timeStyle: "short",
  })
}

/**
 * KPIs sobre el conjunto de filas devuelto (p. ej. página actual).
 * ``total_comprado`` = neto (factura/boleta − NC). El ticket global Bsale es
 * monto de ventas reales / nº de ventas reales, **no** neto ÷ compras.
 */
function kpisFromConsolidated(rows: DistribuidoraClientConsolidated[]) {
  const n = rows.length
  let totalVentas = 0
  let totalCompras = 0
  /** Suma de (ticket_cliente × compras_cliente) = suma montos bruto solo 1+6 en este conjunto. */
  let sumaVentasRealesParaTicket = 0
  for (const r of rows) {
    const compras = Number(r.total_compras ?? 0)
    const ticket = Number(r.ticket_promedio ?? 0)
    totalVentas += Number(r.total_comprado ?? 0)
    totalCompras += compras
    sumaVentasRealesParaTicket += ticket * compras
  }
  const ticketPromedioGlobal =
    totalCompras > 0 ? sumaVentasRealesParaTicket / totalCompras : 0
  const comprasPorCliente = n > 0 ? totalCompras / n : 0
  return {
    totalClientes: n,
    totalVentas,
    ticketPromedioGlobal,
    comprasPorCliente,
  }
}

function frequencyRowClass(dias: number): string {
  if (!Number.isFinite(dias)) return ""
  if (dias <= 14) {
    return "bg-emerald-50/80 dark:bg-emerald-950/25 border-l-4 border-l-emerald-500"
  }
  if (dias <= 45) {
    return "bg-amber-50/80 dark:bg-amber-950/25 border-l-4 border-l-amber-500"
  }
  return "bg-red-50/80 dark:bg-red-950/25 border-l-4 border-l-red-500"
}

export default function DistribuidoraClientesDashboardPage() {
  const defaultTo = localIsoDate()
  const defaultFrom = addDaysIso(defaultTo, -30)

  const [dateFrom, setDateFrom] = useState(defaultFrom)
  const [dateTo, setDateTo] = useState(defaultTo)
  const [seller, setSeller] = useState<string>(ALL_VALUE)
  const [municipality, setMunicipality] = useState<string>(ALL_VALUE)

  const [appliedFrom, setAppliedFrom] = useState(defaultFrom)
  const [appliedTo, setAppliedTo] = useState(defaultTo)
  const [appliedSeller, setAppliedSeller] = useState<string | undefined>(undefined)
  const [appliedMuni, setAppliedMuni] = useState<string | undefined>(undefined)

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [consolidated, setConsolidated] = useState<DistribuidoraClientConsolidated[]>([])
  const [top, setTop] = useState<DistribuidoraClientTop[]>([])
  const [inactive, setInactive] = useState<DistribuidoraClientInactive[]>([])
  const [frequency, setFrequency] = useState<DistribuidoraClientFrequency[]>([])
  const [sellers, setSellers] = useState<DistribuidoraClientSellerSummary[]>([])
  const [sellerOptions, setSellerOptions] = useState<string[]>([])
  const [muniOptions, setMuniOptions] = useState<string[]>([])

  const commonQuery = useMemo(
    () => ({
      start_date: appliedFrom,
      end_date: appliedTo,
      seller: appliedSeller,
      municipality: appliedMuni,
    }),
    [appliedFrom, appliedTo, appliedSeller, appliedMuni],
  )

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [
        consolidatedRes,
        topRes,
        inactiveRes,
        frequencyRes,
        sellersRes,
        salesRes,
      ] = await Promise.all([
        getDistribuidoraClientsConsolidated({
          ...commonQuery,
          limit: 1000,
          offset: 0,
        }),
        getDistribuidoraClientsTop({ limit: 100 }),
        getDistribuidoraClientsInactive({ days: 7, limit: 1000 }),
        getDistribuidoraClientsFrequency({
          ...commonQuery,
          limit: 1000,
        }),
        getDistribuidoraClientsSummarySellers({ limit: 500 }),
        getDistribuidoraSales({
          ...commonQuery,
          limit: 2000,
          offset: 0,
        }),
      ])

      setConsolidated(consolidatedRes.items)
      setTop(topRes.items)
      setInactive(inactiveRes.items)
      setFrequency(frequencyRes.items)
      setSellers(sellersRes.items)

      const sOpts = Array.from(
        new Set(
          sellersRes.items
            .map((r) => (r.seller_name ?? "").trim())
            .filter(Boolean),
        ),
      ).sort((a, b) => a.localeCompare(b, "es"))
      setSellerOptions(sOpts)

      const mOpts = Array.from(
        new Set(
          salesRes.items
            .map((r) => (r.municipality ?? "").trim())
            .filter(Boolean),
        ),
      ).sort((a, b) => a.localeCompare(b, "es"))
      setMuniOptions(mOpts)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar datos")
    } finally {
      setLoading(false)
    }
  }, [commonQuery])

  useEffect(() => {
    void load()
  }, [load])

  const kpi = useMemo(() => kpisFromConsolidated(consolidated), [consolidated])

  const onApplyFilters = () => {
    let from = dateFrom
    let to = dateTo
    if (from > to) {
      const t = from
      from = to
      to = t
      setDateFrom(from)
      setDateTo(to)
    }
    setAppliedFrom(from)
    setAppliedTo(to)
    setAppliedSeller(seller === ALL_VALUE ? undefined : seller)
    setAppliedMuni(municipality === ALL_VALUE ? undefined : municipality)
  }

  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-8 pb-12">
      <div>
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Distribuidora
        </p>
        <h1 className="mt-1 text-2xl font-semibold tracking-tight">Análisis de clientes</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
          Dashboard comercial: prioridad a inactivos, frecuencia de compra y desempeño por vendedor.
        </p>
      </div>

      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-base">Filtros</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4 lg:items-end">
          <div className="space-y-2">
            <Label htmlFor="df">Fecha desde</Label>
            <Input
              id="df"
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="dt">Fecha hasta</Label>
            <Input
              id="dt"
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label>Vendedor</Label>
            <Select value={seller} onValueChange={setSeller}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL_VALUE}>Todos</SelectItem>
                {sellerOptions.map((s) => (
                  <SelectItem key={s} value={s}>
                    {s}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Ciudad / comuna</Label>
            <Select value={municipality} onValueChange={setMunicipality}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent className="max-h-64">
                <SelectItem value={ALL_VALUE}>Todas</SelectItem>
                {muniOptions.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex flex-wrap gap-2 sm:col-span-2 lg:col-span-4">
            <Button type="button" onClick={onApplyFilters} disabled={loading}>
              Aplicar filtros
            </Button>
            <Button
              type="button"
              variant="outline"
              size="icon"
              onClick={() => void load()}
              disabled={loading}
              aria-label="Recargar"
            >
              <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
            </Button>
          </div>
        </CardContent>
      </Card>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex min-h-[200px] items-center justify-center gap-2 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span>Cargando…</span>
        </div>
      ) : (
        <>
          <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total clientes</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{kpi.totalClientes}</p>
                <p className="text-xs text-muted-foreground">En rango consolidado (máx. 1000 filas)</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total ventas</CardTitle>
                <Wallet className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{formatCLP(kpi.totalVentas)}</p>
                <p className="text-xs text-muted-foreground">Suma total comprado</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Ticket promedio</CardTitle>
                <ShoppingBag className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">
                  {formatCLP(kpi.ticketPromedioGlobal)}
                </p>
                <p className="text-xs text-muted-foreground">Ventas / nº compras</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Compras / cliente</CardTitle>
                <Timer className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">
                  {kpi.comprasPorCliente.toLocaleString("es-CL", { maximumFractionDigits: 1 })}
                </p>
                <p className="text-xs text-muted-foreground">Promedio en el consolidado</p>
              </CardContent>
            </Card>
          </section>

          <div className="grid gap-6 xl:grid-cols-2">
            <Card className="min-w-0">
              <CardHeader>
                <CardTitle className="text-base">Top clientes</CardTitle>
                <p className="text-sm text-muted-foreground">Por monto total (histórico global del endpoint)</p>
              </CardHeader>
              <CardContent>
                <div className="max-h-[420px] overflow-auto rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Cliente</TableHead>
                        <TableHead className="text-right">Total comprado</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {top.map((r) => (
                        <TableRow key={r.client_id}>
                          <TableCell className="font-medium">
                            {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.total ?? 0))}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card className="min-w-0 border-orange-200/80 dark:border-orange-900/50">
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  Clientes inactivos
                  <span className="rounded bg-orange-500 px-1.5 py-0.5 text-xs font-semibold text-white">
                    &gt; 7 días sin comprar
                  </span>
                </CardTitle>
                <p className="text-sm text-muted-foreground">
                  Filas en rojo: más de 10 días sin comprar (prioridad comercial).
                </p>
              </CardHeader>
              <CardContent>
                <div className="max-h-[420px] overflow-auto rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Cliente</TableHead>
                        <TableHead>Última compra</TableHead>
                        <TableHead className="text-right">Días sin comprar</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {inactive.map((r) => {
                        const d = Number(r.dias_sin_comprar ?? 0)
                        const hot = d > 10
                        return (
                          <TableRow
                            key={r.client_id}
                            className={cn(
                              hot &&
                                "bg-red-50 text-red-950 dark:bg-red-950/40 dark:text-red-50",
                            )}
                          >
                            <TableCell className="font-medium">
                              {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                            </TableCell>
                            <TableCell className="tabular-nums text-muted-foreground">
                              {formatDateTime(r.ultima_compra)}
                            </TableCell>
                            <TableCell className="text-right font-semibold tabular-nums">{d}</TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </div>

          <div className="grid gap-6 xl:grid-cols-2">
            <Card className="min-w-0">
              <CardHeader>
                <CardTitle className="text-base">Frecuencia de compra</CardTitle>
                <p className="text-sm text-muted-foreground">
                  Días promedio entre primera y última venta en el período, dividido por nº de compras.
                  Verde = más seguido, amarillo = medio, rojo = menos frecuente.
                </p>
              </CardHeader>
              <CardContent>
                <div className="max-h-[420px] overflow-auto rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Cliente</TableHead>
                        <TableHead className="text-right">Compras</TableHead>
                        <TableHead className="text-right">Frecuencia (días)</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {frequency.map((r) => {
                        const f = Number(r.frecuencia_dias ?? 0)
                        return (
                          <TableRow key={r.client_id} className={cn(frequencyRowClass(f))}>
                            <TableCell className="font-medium">
                              {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                            </TableCell>
                            <TableCell className="text-right tabular-nums">
                              {r.compras ?? 0}
                            </TableCell>
                            <TableCell className="text-right tabular-nums">
                              {f.toLocaleString("es-CL", { maximumFractionDigits: 1 })}
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card className="min-w-0">
              <CardHeader>
                <CardTitle className="text-base">Resumen por vendedor</CardTitle>
                <p className="text-sm text-muted-foreground">Clientes distintos y ventas en ventas (boleta/factura)</p>
              </CardHeader>
              <CardContent>
                <div className="max-h-[420px] overflow-auto rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Vendedor</TableHead>
                        <TableHead className="text-right">Clientes</TableHead>
                        <TableHead className="text-right">Ventas</TableHead>
                        <TableHead className="text-right">Ticket prom.</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sellers.map((r, i) => (
                        <TableRow key={`${r.seller_name ?? "—"}-${i}`}>
                          <TableCell className="font-medium">
                            {(r.seller_name ?? "").trim() || "—"}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{r.clientes ?? 0}</TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.ventas ?? 0))}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.ticket_promedio ?? 0))}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  )
}
