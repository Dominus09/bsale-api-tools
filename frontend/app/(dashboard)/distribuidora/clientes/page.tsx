"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { ArrowDownAZ, ArrowUpAZ, Loader2, RefreshCw, ShoppingBag, Timer, Users, Wallet } from "lucide-react"

import {
  getDistribuidoraClientsConsolidated,
  getDistribuidoraClientsFrequency,
  getDistribuidoraClientsSummarySellers,
  getDistribuidoraSales,
  type DistribuidoraClientConsolidated,
  type DistribuidoraClientFrequency,
} from "@/lib/api"
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
import { cn } from "@/lib/utils"

const ALL_VALUE = "__todos__"

type SortKey =
  | "client_name"
  | "total_comprado"
  | "frecuencia_dias"
  | "ultima_compra"
  | "vendedor"
  | "total_compras"

type MergedRow = DistribuidoraClientConsolidated & {
  frecuencia_dias: number | null
}

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

function kpisFromConsolidated(rows: DistribuidoraClientConsolidated[]) {
  const n = rows.length
  let totalVentas = 0
  let totalCompras = 0
  let sumaVentasRealesParaTicket = 0
  for (const r of rows) {
    const compras = Number(r.total_compras ?? 0)
    const ticket = Number(r.ticket_promedio ?? 0)
    totalVentas += Number(r.total_comprado ?? 0)
    totalCompras += compras
    sumaVentasRealesParaTicket += ticket * compras
  }
  const ticketPromedioGlobal = totalCompras > 0 ? sumaVentasRealesParaTicket / totalCompras : 0
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

function SortIcon({ active, dir }: { active: boolean; dir: "asc" | "desc" }) {
  if (!active) return <ArrowDownAZ className="ml-1 h-3.5 w-3.5 opacity-40" />
  return dir === "asc" ? (
    <ArrowUpAZ className="ml-1 h-3.5 w-3.5" />
  ) : (
    <ArrowDownAZ className="ml-1 h-3.5 w-3.5" />
  )
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

  const [sortKey, setSortKey] = useState<SortKey>("total_comprado")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc")

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [consolidated, setConsolidated] = useState<DistribuidoraClientConsolidated[]>([])
  const [frequency, setFrequency] = useState<DistribuidoraClientFrequency[]>([])
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
      const [consolidatedRes, frequencyRes, sellersRes, salesRes] = await Promise.all([
        getDistribuidoraClientsConsolidated({
          ...commonQuery,
          limit: 2000,
          offset: 0,
        }),
        getDistribuidoraClientsFrequency({
          ...commonQuery,
          limit: 2000,
        }),
        getDistribuidoraClientsSummarySellers({
          limit: 500,
          start_date: appliedFrom,
          end_date: appliedTo,
        }),
        getDistribuidoraSales({
          ...commonQuery,
          limit: 2000,
          offset: 0,
        }),
      ])

      setConsolidated(consolidatedRes.items)
      setFrequency(frequencyRes.items)

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
  }, [commonQuery, appliedFrom, appliedTo])

  useEffect(() => {
    void load()
  }, [load])

  const mergedRows: MergedRow[] = useMemo(() => {
    const freq = new Map(frequency.map((f) => [f.client_id, f]))
    return consolidated.map((c) => ({
      ...c,
      frecuencia_dias:
        freq.get(c.client_id)?.frecuencia_dias != null
          ? Number(freq.get(c.client_id)!.frecuencia_dias)
          : null,
    }))
  }, [consolidated, frequency])

  const sortedRows = useMemo(() => {
    const rows = [...mergedRows]
    const dir = sortDir === "asc" ? 1 : -1
    rows.sort((a, b) => {
      const cmpStr = (x: string, y: string) => (x < y ? -1 : x > y ? 1 : 0)
      switch (sortKey) {
        case "client_name": {
          const x = (a.client_name ?? "").toLowerCase()
          const y = (b.client_name ?? "").toLowerCase()
          return dir * cmpStr(x, y)
        }
        case "vendedor": {
          const x = (a.vendedor ?? "").toLowerCase()
          const y = (b.vendedor ?? "").toLowerCase()
          return dir * cmpStr(x, y)
        }
        case "ultima_compra": {
          const ta = a.ultima_compra ? new Date(a.ultima_compra).getTime() : 0
          const tb = b.ultima_compra ? new Date(b.ultima_compra).getTime() : 0
          return dir * (ta === tb ? 0 : ta < tb ? -1 : 1)
        }
        case "frecuencia_dias": {
          const fa = a.frecuencia_dias ?? -1
          const fb = b.frecuencia_dias ?? -1
          return dir * (fa === fb ? 0 : fa < fb ? -1 : 1)
        }
        case "total_compras": {
          const ca = Number(a.total_compras ?? 0)
          const cb = Number(b.total_compras ?? 0)
          return dir * (ca === cb ? 0 : ca < cb ? -1 : 1)
        }
        case "total_comprado":
        default: {
          const va = Number(a.total_comprado ?? 0)
          const vb = Number(b.total_comprado ?? 0)
          return dir * (va === vb ? 0 : va < vb ? -1 : 1)
        }
      }
    })
    return rows
  }, [mergedRows, sortKey, sortDir])

  const toggleSort = (k: SortKey) => {
    if (sortKey === k) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortKey(k)
      setSortDir(k === "client_name" || k === "vendedor" ? "asc" : "desc")
    }
  }

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
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora
          </p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight">Cartera de clientes</h1>
          <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
            Operación comercial: montos en el período, frecuencia y vendedor de la última venta. Use columnas para
            ordenar.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/dashboard">Dashboard</Link>
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/clientes/inactivos">Inactivos</Link>
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/vendedores">Vendedores</Link>
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-base">Filtros</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4 lg:items-end">
          <div className="space-y-2">
            <Label htmlFor="df">Fecha desde</Label>
            <Input id="df" type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="dt">Fecha hasta</Label>
            <Input id="dt" type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
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
            <Button type="button" variant="outline" size="icon" onClick={() => void load()} disabled={loading}>
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
                <CardTitle className="text-sm font-medium">Clientes (filas)</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{kpi.totalClientes}</p>
                <p className="text-xs text-muted-foreground">En el consolidado cargado</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total comprado (período)</CardTitle>
                <Wallet className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{formatCLP(kpi.totalVentas)}</p>
                <p className="text-xs text-muted-foreground">Neto con NC</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Ticket promedio</CardTitle>
                <ShoppingBag className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{formatCLP(kpi.ticketPromedioGlobal)}</p>
                <p className="text-xs text-muted-foreground">Solo ventas 1+6 en el conjunto</p>
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
                <p className="text-xs text-muted-foreground">Promedio documentos de venta</p>
              </CardContent>
            </Card>
          </section>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Cartera en el período</CardTitle>
              <p className="text-sm text-muted-foreground">
                Fila = cliente. Frecuencia = días entre primera y última venta en el período ÷ nº compras. Colores en
                frecuencia: verde ágil, ámbar atención, rojo riesgo.
              </p>
            </CardHeader>
            <CardContent>
              <div className="max-h-[min(70vh,720px)] overflow-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>
                        <button
                          type="button"
                          className="inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("client_name")}
                        >
                          Cliente
                          <SortIcon active={sortKey === "client_name"} dir={sortDir} />
                        </button>
                      </TableHead>
                      <TableHead className="text-right">
                        <button
                          type="button"
                          className="ml-auto inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("total_comprado")}
                        >
                          Total comprado
                          <SortIcon active={sortKey === "total_comprado"} dir={sortDir} />
                        </button>
                      </TableHead>
                      <TableHead className="text-right">
                        <button
                          type="button"
                          className="ml-auto inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("frecuencia_dias")}
                        >
                          Frecuencia (días)
                          <SortIcon active={sortKey === "frecuencia_dias"} dir={sortDir} />
                        </button>
                      </TableHead>
                      <TableHead>
                        <button
                          type="button"
                          className="inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("ultima_compra")}
                        >
                          Última compra
                          <SortIcon active={sortKey === "ultima_compra"} dir={sortDir} />
                        </button>
                      </TableHead>
                      <TableHead>
                        <button
                          type="button"
                          className="inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("vendedor")}
                        >
                          Vendedor
                          <SortIcon active={sortKey === "vendedor"} dir={sortDir} />
                        </button>
                      </TableHead>
                      <TableHead className="text-right">
                        <button
                          type="button"
                          className="ml-auto inline-flex items-center font-semibold hover:text-primary"
                          onClick={() => toggleSort("total_compras")}
                        >
                          Nº compras
                          <SortIcon active={sortKey === "total_compras"} dir={sortDir} />
                        </button>
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sortedRows.map((r) => {
                      const f = r.frecuencia_dias != null ? Number(r.frecuencia_dias) : NaN
                      return (
                        <TableRow key={r.client_id} className={cn(Number.isFinite(f) && frequencyRowClass(f))}>
                          <TableCell className="font-medium">
                            {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.total_comprado ?? 0))}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">
                            {Number.isFinite(f)
                              ? f.toLocaleString("es-CL", { maximumFractionDigits: 1 })
                              : "—"}
                          </TableCell>
                          <TableCell className="tabular-nums text-muted-foreground">
                            {formatDateTime(r.ultima_compra)}
                          </TableCell>
                          <TableCell>{(r.vendedor ?? "").trim() || "—"}</TableCell>
                          <TableCell className="text-right tabular-nums">{r.total_compras ?? 0}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
