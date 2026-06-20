"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { Loader2, Search } from "lucide-react"

import { listDistribuidoraCuadraturas, type CuadraturaListItem } from "@/lib/api"
import { operationalStatusBadge } from "@/lib/dispatch-plan-cuadratura"
import { formatClp } from "@/lib/ors-map-ui"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

type FilterKey = "all" | "pending" | "squared" | "with_diff"

const FILTERS: { key: FilterKey; label: string }[] = [
  { key: "pending", label: "Pendientes" },
  { key: "squared", label: "Cuadradas" },
  { key: "with_diff", label: "Con diferencia" },
  { key: "all", label: "Todas" },
]

export default function CuadraturasListPage() {
  const [filter, setFilter] = useState<FilterKey>("pending")
  const [search, setSearch] = useState("")
  const [searchInput, setSearchInput] = useState("")
  const [items, setItems] = useState<CuadraturaListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await listDistribuidoraCuadraturas({
        status: filter,
        search: search || undefined,
        limit: 120,
      })
      setItems(res.items ?? [])
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar cuadraturas")
      setItems([])
    } finally {
      setLoading(false)
    }
  }, [filter, search])

  useEffect(() => {
    void load()
  }, [load])

  const filteredCount = useMemo(() => items.length, [items])

  return (
    <div className="space-y-6 p-6">
      <div>
        <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Distribuidora · Operaciones
        </p>
        <h1 className="text-xl font-semibold tracking-tight">Cuadraturas</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Cuadratura documental sobre picking congelado — medios de pago, NC y no cargados por
          producto.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {FILTERS.map((f) => (
          <Button
            key={f.key}
            type="button"
            size="sm"
            variant={filter === f.key ? "default" : "outline"}
            onClick={() => setFilter(f.key)}
          >
            {f.label}
          </Button>
        ))}
      </div>

      <form
        className="flex max-w-md gap-2"
        onSubmit={(e) => {
          e.preventDefault()
          setSearch(searchInput.trim())
        }}
      >
        <div className="relative flex-1">
          <Search className="absolute left-2.5 top-2.5 size-4 text-muted-foreground" />
          <Input
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Plan, chofer, vehículo o fecha…"
            className="pl-9"
          />
        </div>
        <Button type="submit" variant="secondary">
          Buscar
        </Button>
      </form>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando cuadraturas…
        </div>
      ) : items.length === 0 ? (
        <p className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
          No hay cuadraturas para este filtro.
        </p>
      ) : (
        <>
          <p className="text-xs text-muted-foreground">{filteredCount} rutas con picking</p>
          <div className="overflow-hidden rounded-lg border border-border/80">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Plan</TableHead>
                  <TableHead>Fecha</TableHead>
                  <TableHead>Vehículo</TableHead>
                  <TableHead>Chofer</TableHead>
                  <TableHead className="text-right">Venta picking</TableHead>
                  <TableHead className="text-right">Recaudado</TableHead>
                  <TableHead className="text-right">Diferencia</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((row) => {
                  const st = operationalStatusBadge(row.cuadratura_status || "pending")
                  return (
                    <TableRow key={row.dispatch_plan_id}>
                      <TableCell className="font-mono text-xs">
                        {row.planning_code || `PLAN-${row.dispatch_plan_id}`}
                      </TableCell>
                      <TableCell className="text-xs">
                        {row.planning_date?.slice(0, 10) ?? "—"}
                      </TableCell>
                      <TableCell>{row.truck_name || row.route_name || "—"}</TableCell>
                      <TableCell>{row.driver_name || "—"}</TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {formatClp(row.venta_picking_clp ?? 0)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {formatClp(row.total_recaudado_clp ?? 0)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {formatClp(row.diferencia_clp ?? 0)}
                      </TableCell>
                      <TableCell>
                        <Badge className={cn("font-normal", st.className)}>
                          {st.emoji} {st.label}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Button asChild variant="outline" size="sm">
                          <Link href={`/distribuidora/cuadraturas/${row.dispatch_plan_id}`}>
                            Abrir
                          </Link>
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        </>
      )}
    </div>
  )
}
