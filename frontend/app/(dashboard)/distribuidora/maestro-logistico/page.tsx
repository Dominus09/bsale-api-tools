"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Check, Loader2, Search } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
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
  getProductsMaster,
  getProductsMasterLogisticsStats,
  getSuppliers,
  patchProductMaster,
  PRODUCTS_MASTER_PAGE_SIZE,
  type GetProductsMasterParams,
  type ProductMasterLogisticsPatch,
  type ProductMasterRow,
  type ProductsMasterLogisticsStats,
  type Supplier,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const NONE_SELECT = "__none__"
const FILTER_ALL_SUPPLIERS = "__all__"

type LogisticsField =
  | "units_per_box"
  | "weight_box_kg"
  | "height_cm"
  | "width_cm"
  | "length_cm"

function formatCount(n: number): string {
  return n.toLocaleString("es-CL")
}

function productLabel(row: ProductMasterRow): string {
  const pn = (row.product_name || "").trim()
  const vn = (row.variant_name || "").trim()
  if (pn && vn && pn !== vn) return `${pn} — ${vn}`
  return pn || vn || "—"
}

function formatSyncAt(iso: string | null | undefined): string {
  if (!iso) return "—"
  try {
    return new Date(iso).toLocaleString("es-CL", {
      dateStyle: "short",
      timeStyle: "short",
    })
  } catch {
    return iso
  }
}

function parseOptionalNumber(raw: string): number | null | undefined {
  const t = raw.trim()
  if (t === "") return undefined
  const n = Number.parseFloat(t.replace(",", "."))
  if (!Number.isFinite(n) || n < 0) return null
  return n
}

function logisticsPayloadFromRow(row: ProductMasterRow): ProductMasterLogisticsPatch {
  const hasCxC = row.units_per_box != null && row.units_per_box > 0
  const hasPeso = row.weight_box_kg != null && row.weight_box_kg > 0
  const hasAlto = row.height_cm != null && row.height_cm > 0
  const hasAncho = row.width_cm != null && row.width_cm > 0
  const hasLargo = row.length_cm != null && row.length_cm > 0
  return {
    logistics_completed: hasCxC && hasPeso && hasAlto && hasAncho && hasLargo,
  }
}

function InlineNumberInput({
  value,
  disabled,
  className,
  placeholder,
  onCommit,
}: {
  value: number | null | undefined
  disabled?: boolean
  className?: string
  placeholder?: string
  onCommit: (parsed: number | null | undefined) => void
}) {
  const [local, setLocal] = useState(
    value != null && Number.isFinite(value) ? String(value) : "",
  )

  useEffect(() => {
    setLocal(value != null && Number.isFinite(value) ? String(value) : "")
  }, [value])

  return (
    <Input
      type="text"
      inputMode="decimal"
      className={cn("h-8 w-[5.5rem] tabular-nums", className)}
      placeholder={placeholder}
      disabled={disabled}
      value={local}
      onChange={(e) => setLocal(e.target.value)}
      onBlur={() => {
        const parsed = parseOptionalNumber(local)
        const prev =
          value != null && Number.isFinite(value) ? value : undefined
        if (parsed === undefined && prev === undefined) return
        if (parsed != null && prev != null && parsed === prev) return
        onCommit(parsed)
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter") (e.target as HTMLInputElement).blur()
      }}
    />
  )
}

export default function MaestroLogisticoPage() {
  const [rows, setRows] = useState<ProductMasterRow[]>([])
  const [suppliers, setSuppliers] = useState<Supplier[]>([])
  const [loading, setLoading] = useState(true)
  const [suppliersLoading, setSuppliersLoading] = useState(true)
  const [error, setError] = useState("")
  const [suppliersError, setSuppliersError] = useState("")
  const [pendingBarcodes, setPendingBarcodes] = useState<Set<string>>(() => new Set())
  const [rowErrors, setRowErrors] = useState<Record<string, string>>({})
  const [saveSuccessBarcodes, setSaveSuccessBarcodes] = useState<Set<string>>(() => new Set())
  const saveFlashTimeoutsRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

  const [filterLogisticsIncomplete, setFilterLogisticsIncomplete] = useState(false)
  const [filterSupplierId, setFilterSupplierId] = useState<string>(FILTER_ALL_SUPPLIERS)
  const [searchInput, setSearchInput] = useState("")
  const [debouncedSearch, setDebouncedSearch] = useState("")
  const [page, setPage] = useState(0)
  const [total, setTotal] = useState(0)

  const [stats, setStats] = useState<ProductsMasterLogisticsStats | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState("")

  const clearSaveSuccessFlash = useCallback((barcode: string) => {
    const id = saveFlashTimeoutsRef.current.get(barcode)
    if (id !== undefined) {
      window.clearTimeout(id)
      saveFlashTimeoutsRef.current.delete(barcode)
    }
    setSaveSuccessBarcodes((prev) => {
      if (!prev.has(barcode)) return prev
      const next = new Set(prev)
      next.delete(barcode)
      return next
    })
  }, [])

  const flashSaveSuccess = useCallback(
    (barcode: string) => {
      const m = saveFlashTimeoutsRef.current
      const prevId = m.get(barcode)
      if (prevId !== undefined) window.clearTimeout(prevId)
      setSaveSuccessBarcodes((prev) => {
        const next = new Set(prev)
        next.add(barcode)
        return next
      })
      const tid = window.setTimeout(() => {
        setSaveSuccessBarcodes((prev) => {
          const next = new Set(prev)
          next.delete(barcode)
          return next
        })
        m.delete(barcode)
      }, 2000)
      m.set(barcode, tid)
    },
    [],
  )

  useEffect(() => {
    return () => {
      saveFlashTimeoutsRef.current.forEach((id) => window.clearTimeout(id))
      saveFlashTimeoutsRef.current.clear()
    }
  }, [])

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedSearch(searchInput.trim()), 400)
    return () => window.clearTimeout(t)
  }, [searchInput])

  const filterQueryParams = useMemo((): GetProductsMasterParams => {
    const p: GetProductsMasterParams = {}
    if (debouncedSearch) p.search = debouncedSearch
    if (filterLogisticsIncomplete) p.logistics_incomplete = true
    if (filterSupplierId !== FILTER_ALL_SUPPLIERS) {
      const id = Number.parseInt(filterSupplierId, 10)
      if (Number.isFinite(id)) p.supplier_id = id
    }
    return p
  }, [debouncedSearch, filterLogisticsIncomplete, filterSupplierId])

  useEffect(() => {
    setPage(0)
  }, [debouncedSearch, filterLogisticsIncomplete, filterSupplierId])

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError("")
      try {
        const data = await getProductsMaster({
          ...filterQueryParams,
          limit: PRODUCTS_MASTER_PAGE_SIZE,
          offset: page * PRODUCTS_MASTER_PAGE_SIZE,
        })
        if (!cancelled) {
          setRows(data.items)
          setTotal(data.total)
        }
      } catch {
        if (!cancelled) {
          setError("No se pudieron cargar los productos")
          setRows([])
          setTotal(0)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [filterQueryParams, page])

  useEffect(() => {
    let cancelled = false
    async function loadStats() {
      setStatsLoading(true)
      setStatsError("")
      try {
        const s = await getProductsMasterLogisticsStats()
        if (!cancelled) setStats(s)
      } catch {
        if (!cancelled) {
          setStats(null)
          setStatsError("No se pudieron cargar los KPIs")
        }
      } finally {
        if (!cancelled) setStatsLoading(false)
      }
    }
    loadStats()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    async function loadSuppliers() {
      setSuppliersLoading(true)
      setSuppliersError("")
      try {
        const list = await getSuppliers()
        if (!cancelled) {
          setSuppliers(
            [...list].sort((a, b) =>
              a.name.localeCompare(b.name, "es", { sensitivity: "base" }),
            ),
          )
        }
      } catch {
        if (!cancelled) {
          setSuppliers([])
          setSuppliersError("No se pudieron cargar los proveedores")
        }
      } finally {
        if (!cancelled) setSuppliersLoading(false)
      }
    }
    loadSuppliers()
    return () => {
      cancelled = true
    }
  }, [])

  const setPending = useCallback((barcode: string, on: boolean) => {
    setPendingBarcodes((prev) => {
      const next = new Set(prev)
      if (on) next.add(barcode)
      else next.delete(barcode)
      return next
    })
  }, [])

  const patchRow = useCallback(
    async (
      row: ProductMasterRow,
      patch: ProductMasterLogisticsPatch,
      optimistic: Partial<ProductMasterRow>,
    ) => {
      setRowErrors((e) => {
        const copy = { ...e }
        delete copy[row.barcode]
        return copy
      })
      clearSaveSuccessFlash(row.barcode)
      setPending(row.barcode, true)
      const prev = row
      setRows((rs) =>
        rs.map((r) => (r.barcode === row.barcode ? { ...r, ...optimistic } : r)),
      )
      try {
        const updated = await patchProductMaster(row.barcode, patch)
        setRows((rs) =>
          rs.map((r) =>
            r.barcode === row.barcode
              ? {
                  ...r,
                  supplier_id: updated.supplier_id ?? r.supplier_id,
                  units_per_box: updated.units_per_box ?? r.units_per_box,
                  weight_box_kg: updated.weight_box_kg ?? r.weight_box_kg,
                  height_cm: updated.height_cm ?? r.height_cm,
                  width_cm: updated.width_cm ?? r.width_cm,
                  length_cm: updated.length_cm ?? r.length_cm,
                  weight_unit_kg: updated.weight_unit_kg ?? r.weight_unit_kg,
                  volume_m3: updated.volume_m3 ?? r.volume_m3,
                  logistics_completed:
                    updated.logistics_completed ?? r.logistics_completed,
                  updated_at: updated.updated_at ?? r.updated_at,
                }
              : r,
          ),
        )
        flashSaveSuccess(row.barcode)
      } catch {
        setRows((rs) =>
          rs.map((r) => (r.barcode === row.barcode ? prev : r)),
        )
        setRowErrors((e) => ({
          ...e,
          [row.barcode]: "No se pudo guardar",
        }))
      } finally {
        setPending(row.barcode, false)
      }
    },
    [clearSaveSuccessFlash, flashSaveSuccess, setPending],
  )

  const handleLogisticsField = useCallback(
    async (row: ProductMasterRow, field: LogisticsField, parsed: number | null | undefined) => {
      if (parsed === undefined) return
      if (parsed === null) return
      const patch: ProductMasterLogisticsPatch = { [field]: parsed }
      const optimistic: Partial<ProductMasterRow> = { [field]: parsed }
      const nextRow = { ...row, ...optimistic }
      Object.assign(patch, logisticsPayloadFromRow(nextRow))
      if (patch.logistics_completed != null) {
        optimistic.logistics_completed = patch.logistics_completed
      }
      await patchRow(row, patch, optimistic)
    },
    [patchRow],
  )

  const handleSupplierChange = useCallback(
    async (row: ProductMasterRow, value: string) => {
      const nextId: number | null =
        value === NONE_SELECT ? null : Number.parseInt(value, 10)
      if (value !== NONE_SELECT && !Number.isFinite(nextId)) return
      if (row.supplier_id === nextId) return
      await patchRow(row, { supplier_id: nextId }, { supplier_id: nextId })
    },
    [patchRow],
  )

  const pageFrom = total === 0 ? 0 : page * PRODUCTS_MASTER_PAGE_SIZE + 1
  const pageTo = Math.min((page + 1) * PRODUCTS_MASTER_PAGE_SIZE, total)
  const canPrev = page > 0 && !loading
  const canNext = !loading && (page + 1) * PRODUCTS_MASTER_PAGE_SIZE < total
  const selectDisabled = suppliersLoading || !!suppliersError || suppliers.length === 0

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-foreground">Maestro logístico productos</h1>
        <p className="text-sm text-muted-foreground">
          Datos de cubicación y proveedor por código de barras. Los nombres y CxC se actualizan
          desde Bsale; peso y dimensiones solo se editan aquí.
        </p>
      </div>

      {error ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      ) : null}

      {suppliersError ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {suppliersError}
        </div>
      ) : null}

      {statsError ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {statsError}
        </div>
      ) : null}

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        {(
          [
            ["Productos", stats?.total],
            ["Con proveedor", stats?.with_supplier],
            ["Con CxC", stats?.with_units_per_box],
            ["Con peso", stats?.with_weight],
            ["Con dimensiones", stats?.with_dimensions],
            ["Completitud", stats?.completeness_pct, true],
          ] as const
        ).map(([label, value, isPct]) => (
          <Card key={label} className="shadow-none">
            <CardContent className="py-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                {label}
              </p>
              <p className="mt-1 text-2xl font-semibold tabular-nums text-foreground">
                {statsLoading ? (
                  <Loader2 className="size-5 animate-spin text-muted-foreground" />
                ) : isPct ? (
                  `${value ?? 0}%`
                ) : (
                  formatCount(Number(value ?? 0))
                )}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Catálogo maestro</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-4 border-b border-border pb-4 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="flex items-center gap-2">
              <Checkbox
                id="filter-logistics-incomplete"
                checked={filterLogisticsIncomplete}
                onCheckedChange={(v) => setFilterLogisticsIncomplete(v === true)}
              />
              <Label
                htmlFor="filter-logistics-incomplete"
                className="cursor-pointer text-sm font-medium"
              >
                Solo datos logísticos incompletos
              </Label>
            </div>

            <div className="flex min-w-[200px] flex-1 flex-col gap-1.5 sm:max-w-xs">
              <Label className="text-xs text-muted-foreground">Proveedor</Label>
              <Select
                value={filterSupplierId}
                disabled={suppliersLoading || !!suppliersError}
                onValueChange={setFilterSupplierId}
              >
                <SelectTrigger className="h-9 w-full">
                  <SelectValue placeholder="Todos" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={FILTER_ALL_SUPPLIERS}>Todos</SelectItem>
                  {suppliers.map((s) => (
                    <SelectItem key={s.id} value={String(s.id)}>
                      {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex min-w-[220px] flex-1 flex-col gap-1.5 sm:max-w-md">
              <Label htmlFor="logistics-search" className="text-xs text-muted-foreground">
                Buscar producto o código de barras
              </Label>
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="logistics-search"
                  className="h-9 pl-9"
                  placeholder="Nombre o barcode…"
                  value={searchInput}
                  onChange={(e) => setSearchInput(e.target.value)}
                  autoComplete="off"
                />
              </div>
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full min-w-[1100px]">
              <thead>
                <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                  <th className="pb-3 pr-3">Producto</th>
                  <th className="pb-3 pr-3">Código barra</th>
                  <th className="pb-3 pr-3">CxC</th>
                  <th className="pb-3 pr-3">Peso caja (kg)</th>
                  <th className="pb-3 pr-3">Alto (cm)</th>
                  <th className="pb-3 pr-3">Ancho (cm)</th>
                  <th className="pb-3 pr-3">Largo (cm)</th>
                  <th className="pb-3 pr-3">Proveedor</th>
                  <th className="pb-3">Última sync Bsale</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={9} className="py-6 text-center text-sm text-muted-foreground">
                      Cargando…
                    </td>
                  </tr>
                ) : rows.length === 0 ? (
                  <tr>
                    <td colSpan={9} className="py-6 text-center text-sm text-muted-foreground">
                      No hay productos para mostrar
                    </td>
                  </tr>
                ) : (
                  rows.map((row) => {
                    const pending = pendingBarcodes.has(row.barcode)
                    const showSaved = saveSuccessBarcodes.has(row.barcode)
                    const supplierValue =
                      row.supplier_id != null ? String(row.supplier_id) : NONE_SELECT
                    const orphanSupplier =
                      row.supplier_id != null &&
                      !suppliers.some((s) => s.id === row.supplier_id)

                    return (
                      <tr
                        key={`${row.id}-${row.barcode}`}
                        className={cn(
                          "group border-b border-border last:border-0",
                          pending && "bg-muted/40",
                        )}
                      >
                        <td className="py-2.5 pr-3 align-middle">
                          <div className="max-w-[220px]">
                            <p className="text-sm font-medium leading-snug">
                              {productLabel(row)}
                            </p>
                            {row.weight_unit_kg != null ? (
                              <p className="mt-0.5 text-xs text-muted-foreground tabular-nums">
                                Peso unit. ≈ {row.weight_unit_kg.toFixed(4)} kg
                              </p>
                            ) : null}
                            {row.logistics_completed ? (
                              <Badge variant="secondary" className="mt-1 text-[10px]">
                                Logística OK
                              </Badge>
                            ) : null}
                          </div>
                        </td>
                        <td className="py-2.5 pr-3 align-middle font-mono text-xs">
                          {row.barcode}
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <InlineNumberInput
                            value={row.units_per_box}
                            disabled={pending}
                            placeholder="—"
                            onCommit={(parsed) => {
                              if (parsed === undefined || parsed === null) return
                              const n = Math.round(parsed)
                              if (n < 1) return
                              void handleLogisticsField(row, "units_per_box", n)
                            }}
                          />
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <InlineNumberInput
                            value={row.weight_box_kg}
                            disabled={pending}
                            onCommit={(parsed) => {
                              if (parsed === undefined || parsed === null) return
                              void handleLogisticsField(row, "weight_box_kg", parsed)
                            }}
                          />
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <InlineNumberInput
                            value={row.height_cm}
                            disabled={pending}
                            onCommit={(parsed) => {
                              if (parsed === undefined || parsed === null) return
                              void handleLogisticsField(row, "height_cm", parsed)
                            }}
                          />
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <InlineNumberInput
                            value={row.width_cm}
                            disabled={pending}
                            onCommit={(parsed) => {
                              if (parsed === undefined || parsed === null) return
                              void handleLogisticsField(row, "width_cm", parsed)
                            }}
                          />
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <InlineNumberInput
                            value={row.length_cm}
                            disabled={pending}
                            onCommit={(parsed) => {
                              if (parsed === undefined || parsed === null) return
                              void handleLogisticsField(row, "length_cm", parsed)
                            }}
                          />
                        </td>
                        <td className="py-2.5 pr-3 align-middle">
                          <div className="flex min-w-[180px] items-center gap-1.5">
                            <Select
                              value={supplierValue}
                              disabled={selectDisabled || pending}
                              onValueChange={(v) => void handleSupplierChange(row, v)}
                            >
                              <SelectTrigger className="h-8 w-full min-w-0">
                                <SelectValue placeholder="Proveedor…" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value={NONE_SELECT}>Sin proveedor</SelectItem>
                                {orphanSupplier ? (
                                  <SelectItem value={String(row.supplier_id)}>
                                    Proveedor #{row.supplier_id}
                                  </SelectItem>
                                ) : null}
                                {suppliers.map((s) => (
                                  <SelectItem key={s.id} value={String(s.id)}>
                                    {s.name}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <span className="flex size-6 shrink-0 items-center justify-center">
                              {pending ? (
                                <Loader2 className="size-3.5 animate-spin text-muted-foreground" />
                              ) : showSaved ? (
                                <Check className="size-3.5 text-emerald-600" strokeWidth={2.5} />
                              ) : null}
                            </span>
                          </div>
                          {rowErrors[row.barcode] ? (
                            <p className="mt-0.5 text-xs text-destructive">
                              {rowErrors[row.barcode]}
                            </p>
                          ) : null}
                        </td>
                        <td className="py-2.5 align-middle text-xs text-muted-foreground whitespace-nowrap">
                          {formatSyncAt(row.last_bsale_sync_at)}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>

          <div className="flex flex-col gap-3 border-t border-border pt-4 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm text-muted-foreground">
              Mostrando{" "}
              <span className="font-medium tabular-nums text-foreground">
                {pageFrom}–{pageTo}
              </span>{" "}
              de <span className="font-medium tabular-nums text-foreground">{total}</span>
            </p>
            <div className="flex gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={!canPrev}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
              >
                Anterior
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={!canNext}
                onClick={() => setPage((p) => p + 1)}
              >
                Siguiente
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
