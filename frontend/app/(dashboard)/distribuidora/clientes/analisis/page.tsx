"use client"

import type { ReactNode } from "react"
import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import {
  ArrowDownAZ,
  ArrowUpAZ,
  Download,
  Loader2,
  RefreshCw,
  Sparkles,
} from "lucide-react"

import {
  downloadDistribuidoraClientesAnalisisExcel,
  getDistribuidoraClientesAnalisis,
  type DistribuidoraClienteAnalisis,
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

const ALL = "__todos__"

type SortKey =
  | "fantasy_name"
  | "nombre"
  | "rut_clean"
  | "ciudad"
  | "ultima_compra"
  | "dias_sin_comprar"
  | "compra_30_dias"
  | "compra_60_dias"
  | "freq_enero"
  | "freq_febrero"
  | "freq_marzo"
  | "freq_abril"
  | "nivel_cliente"

function ciudadLabel(r: DistribuidoraClienteAnalisis): string {
  const parts = [r.city, r.municipality].filter((x) => x && String(x).trim())
  return parts.length ? parts.join(" · ") : "—"
}

function formatCLP(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—"
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString("es-CL", { dateStyle: "short" })
}

function nivelClass(nivel: string | null | undefined): string {
  switch (nivel) {
    case "A":
      return "bg-emerald-600/20 text-emerald-900 dark:text-emerald-200 font-semibold"
    case "B":
      return "bg-emerald-400/25 text-emerald-900 dark:text-emerald-100 font-semibold"
    case "C":
      return "bg-amber-400/30 text-amber-950 dark:text-amber-100 font-semibold"
    case "D":
      return "bg-orange-500/25 text-orange-950 dark:text-orange-100 font-semibold"
    case "E":
      return "bg-red-600/20 text-red-900 dark:text-red-200 font-semibold"
    default:
      return ""
  }
}

function compareVal(a: unknown, b: unknown): number {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  if (typeof a === "number" && typeof b === "number") return a - b
  return String(a).localeCompare(String(b), "es", { sensitivity: "base" })
}

function sortKeyValue(row: DistribuidoraClienteAnalisis, key: SortKey): unknown {
  switch (key) {
    case "ciudad":
      return ciudadLabel(row)
    default:
      return row[key as keyof DistribuidoraClienteAnalisis]
  }
}

export default function DistribuidoraClientesAnalisisPage() {
  const [items, setItems] = useState<DistribuidoraClienteAnalisis[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [exporting, setExporting] = useState(false)

  const [search, setSearch] = useState("")
  const [ciudad, setCiudad] = useState(ALL)
  const [nivel, setNivel] = useState(ALL)
  const [minDiasSinComprar, setMinDiasSinComprar] = useState("")
  const [soloSabado, setSoloSabado] = useState(false)

  const [sortKey, setSortKey] = useState<SortKey>("compra_30_dias")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc")

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraClientesAnalisis({ limit: 5000 })
      setItems(res.items ?? [])
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar datos")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const ciudadesOpciones = useMemo(() => {
    const s = new Set<string>()
    for (const r of items) {
      const lab = ciudadLabel(r)
      if (lab !== "—") s.add(lab)
    }
    return Array.from(s).sort((a, b) => a.localeCompare(b, "es"))
  }, [items])

  const filtrados = useMemo(() => {
    const q = search.trim().toLowerCase()
    const minD = minDiasSinComprar.trim() === "" ? null : Number(minDiasSinComprar)
    const minDValid = minD != null && !Number.isNaN(minD)

    return items.filter((r) => {
      if (soloSabado) {
        const n = r.nivel_cliente
        if (n !== "A" && n !== "B") return false
      } else if (nivel !== ALL && r.nivel_cliente !== nivel) return false

      if (ciudad !== ALL && ciudadLabel(r) !== ciudad) return false

      if (minDValid) {
        const d = r.dias_sin_comprar
        if (d == null || d < minD) return false
      }

      if (q) {
        const blob = `${r.fantasy_name ?? ""} ${r.nombre ?? ""} ${r.rut_clean ?? ""}`.toLowerCase()
        if (!blob.includes(q)) return false
      }
      return true
    })
  }, [items, search, ciudad, nivel, minDiasSinComprar, soloSabado])

  const ordenados = useMemo(() => {
    const out = [...filtrados]
    out.sort((a, b) => {
      const va = sortKeyValue(a, sortKey)
      const vb = sortKeyValue(b, sortKey)
      const c = compareVal(va, vb)
      return sortDir === "asc" ? c : -c
    })
    return out
  }, [filtrados, sortKey, sortDir])

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortKey(key)
      setSortDir(key === "nombre" || key === "fantasy_name" || key === "rut_clean" || key === "ciudad" ? "asc" : "desc")
    }
  }

  const SortBtn = ({ k, children }: { k: SortKey; children: ReactNode }) => (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      className="-ml-2 h-8 gap-1 px-2 font-semibold"
      onClick={() => toggleSort(k)}
    >
      {children}
      {sortKey === k ? (
        sortDir === "asc" ? (
          <ArrowUpAZ className="h-3.5 w-3.5 opacity-70" />
        ) : (
          <ArrowDownAZ className="h-3.5 w-3.5 opacity-70" />
        )
      ) : null}
    </Button>
  )

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Link href="/distribuidora/clientes" className="hover:text-foreground">
              Clientes
            </Link>
            <span>/</span>
            <span>Análisis</span>
          </div>
          <h1 className="text-2xl font-semibold tracking-tight">Análisis de clientes</h1>
          <p className="text-muted-foreground text-sm max-w-2xl">
            Comportamiento de compra, frecuencia mensual (año en curso), ventas recientes y nivel A–E. Ideal para
            priorizar visitas y rutas.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            <span className="ml-1">Actualizar</span>
          </Button>
          <Button
            variant={soloSabado ? "default" : "secondary"}
            size="sm"
            onClick={() => setSoloSabado((v) => !v)}
          >
            <Sparkles className="h-4 w-4" />
            <span className="ml-1">Clientes sábado (A–B)</span>
          </Button>
          <Button
            size="sm"
            disabled={exporting}
            onClick={async () => {
              setExporting(true)
              try {
                await downloadDistribuidoraClientesAnalisisExcel({ limit: 10000 })
              } catch (e) {
                setError(e instanceof Error ? e.message : "Error al exportar")
              } finally {
                setExporting(false)
              }
            }}
          >
            {exporting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
            <span className="ml-1">Exportar Excel</span>
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Filtros</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-2">
            <Label htmlFor="buscar">Buscar (nombre / fantasía / RUT)</Label>
            <Input
              id="buscar"
              placeholder="Ej. melinka, 76…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label>Ciudad / comuna</Label>
            <Select value={ciudad} onValueChange={setCiudad}>
              <SelectTrigger>
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todas</SelectItem>
                {ciudadesOpciones.map((c) => (
                  <SelectItem key={c} value={c}>
                    {c}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Nivel cliente</Label>
            <Select value={nivel} onValueChange={setNivel} disabled={soloSabado}>
              <SelectTrigger>
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todos</SelectItem>
                {["A", "B", "C", "D", "E"].map((n) => (
                  <SelectItem key={n} value={n}>
                    {n}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label htmlFor="dias">Mín. días sin comprar</Label>
            <Input
              id="dias"
              type="number"
              min={0}
              placeholder="Ej. 15"
              value={minDiasSinComprar}
              onChange={(e) => setMinDiasSinComprar(e.target.value)}
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between py-3">
          <CardTitle className="text-base">
            Resultados
            <span className="ml-2 text-muted-foreground font-normal text-sm">
              ({ordenados.length} de {items.length})
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="relative max-h-[calc(100vh-20rem)] overflow-auto rounded-b-lg border-t">
            {loading ? (
              <div className="flex items-center justify-center py-24 text-muted-foreground gap-2">
                <Loader2 className="h-6 w-6 animate-spin" />
                Cargando…
              </div>
            ) : (
              <Table>
                <TableHeader className="sticky top-0 z-20 bg-background/95 shadow-sm backdrop-blur supports-[backdrop-filter]:bg-background/80">
                  <TableRow>
                    <TableHead className="whitespace-nowrap min-w-[140px]">
                      <SortBtn k="fantasy_name">Nombre fantasía</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap min-w-[120px]">
                      <SortBtn k="nombre">Nombre</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap">
                      <SortBtn k="rut_clean">RUT</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap min-w-[120px]">
                      <SortBtn k="ciudad">Ciudad</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap">
                      <SortBtn k="ultima_compra">Última compra</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap text-right">
                      <SortBtn k="dias_sin_comprar">Días sin comprar</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap text-right">
                      <SortBtn k="compra_30_dias">Compra 30 días</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap text-right">
                      <SortBtn k="compra_60_dias">Compra 60 días</SortBtn>
                    </TableHead>
                    <TableHead className="text-center whitespace-nowrap">
                      <SortBtn k="freq_enero">Ene</SortBtn>
                    </TableHead>
                    <TableHead className="text-center whitespace-nowrap">
                      <SortBtn k="freq_febrero">Feb</SortBtn>
                    </TableHead>
                    <TableHead className="text-center whitespace-nowrap">
                      <SortBtn k="freq_marzo">Mar</SortBtn>
                    </TableHead>
                    <TableHead className="text-center whitespace-nowrap">
                      <SortBtn k="freq_abril">Abr</SortBtn>
                    </TableHead>
                    <TableHead className="whitespace-nowrap text-center">
                      <SortBtn k="nivel_cliente">Nivel</SortBtn>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {ordenados.map((r) => {
                    const dias = r.dias_sin_comprar
                    const diasAlerta = dias != null && dias > 15
                    return (
                      <TableRow key={r.client_id}>
                        <TableCell className="max-w-[200px] truncate" title={r.fantasy_name ?? ""}>
                          {r.fantasy_name ?? "—"}
                        </TableCell>
                        <TableCell className="max-w-[180px] truncate" title={r.nombre ?? ""}>
                          {r.nombre ?? "—"}
                        </TableCell>
                        <TableCell className="font-mono text-xs">{r.rut_clean ?? "—"}</TableCell>
                        <TableCell className="max-w-[160px] truncate">{ciudadLabel(r)}</TableCell>
                        <TableCell>{formatDate(r.ultima_compra)}</TableCell>
                        <TableCell
                          className={cn(
                            "text-right tabular-nums",
                            diasAlerta && "text-red-600 dark:text-red-400 font-semibold",
                          )}
                        >
                          {dias ?? "—"}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{formatCLP(r.compra_30_dias)}</TableCell>
                        <TableCell className="text-right tabular-nums">{formatCLP(r.compra_60_dias)}</TableCell>
                        <TableCell className="text-center tabular-nums">{r.freq_enero ?? 0}</TableCell>
                        <TableCell className="text-center tabular-nums">{r.freq_febrero ?? 0}</TableCell>
                        <TableCell className="text-center tabular-nums">{r.freq_marzo ?? 0}</TableCell>
                        <TableCell className="text-center tabular-nums">{r.freq_abril ?? 0}</TableCell>
                        <TableCell className="text-center">
                          <span
                            className={cn(
                              "inline-flex min-w-[2rem] justify-center rounded-md px-2 py-0.5 text-sm",
                              nivelClass(r.nivel_cliente),
                            )}
                          >
                            {r.nivel_cliente ?? "—"}
                          </span>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
