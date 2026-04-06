"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Check, Loader2, PackageMinus, Search, Upload } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  getProductsMaster,
  getProductsMasterUnassignedCount,
  getSuppliers,
  patchProductMaster,
  type GetProductsMasterParams,
  type ProductMasterRow,
  type Supplier,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const NONE_SELECT = "__none__"
const FILTER_ALL_SUPPLIERS = "__all__"

export default function Page() {
  const [rows, setRows] = useState<ProductMasterRow[]>([])
  const [suppliers, setSuppliers] = useState<Supplier[]>([])
  const [loading, setLoading] = useState(true)
  const [suppliersLoading, setSuppliersLoading] = useState(true)
  const [error, setError] = useState("")
  const [suppliersError, setSuppliersError] = useState("")
  const [pendingBarcodes, setPendingBarcodes] = useState<Set<string>>(() => new Set())
  const [rowErrors, setRowErrors] = useState<Record<string, string>>({})

  const [filterWithoutSupplier, setFilterWithoutSupplier] = useState(false)
  const [filterSupplierId, setFilterSupplierId] = useState<string>(FILTER_ALL_SUPPLIERS)
  const [searchInput, setSearchInput] = useState("")
  const [debouncedSearch, setDebouncedSearch] = useState("")

  const [unassignedCount, setUnassignedCount] = useState<number | null>(null)
  const [unassignedLoading, setUnassignedLoading] = useState(true)
  const [unassignedError, setUnassignedError] = useState("")

  const [bulkDialogOpen, setBulkDialogOpen] = useState(false)
  const bulkFileInputRef = useRef<HTMLInputElement>(null)
  const [bulkFileName, setBulkFileName] = useState<string | null>(null)

  const [saveSuccessBarcodes, setSaveSuccessBarcodes] = useState<Set<string>>(() => new Set())
  const saveFlashTimeoutsRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

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

  const loadUnassignedCount = useCallback(async () => {
    setUnassignedLoading(true)
    setUnassignedError("")
    try {
      const n = await getProductsMasterUnassignedCount()
      setUnassignedCount(n)
    } catch {
      setUnassignedError("No se pudo cargar el total sin proveedor")
      setUnassignedCount(null)
    } finally {
      setUnassignedLoading(false)
    }
  }, [])

  useEffect(() => {
    loadUnassignedCount()
  }, [loadUnassignedCount])

  useEffect(() => {
    const t = window.setTimeout(() => {
      setDebouncedSearch(searchInput.trim())
    }, 400)
    return () => window.clearTimeout(t)
  }, [searchInput])

  const queryParams = useMemo((): GetProductsMasterParams | undefined => {
    const p: GetProductsMasterParams = {}
    if (debouncedSearch) p.search = debouncedSearch
    if (filterWithoutSupplier) {
      p.without_supplier = true
    } else if (filterSupplierId !== FILTER_ALL_SUPPLIERS) {
      const id = Number.parseInt(filterSupplierId, 10)
      if (Number.isFinite(id)) p.supplier_id = id
    }
    return Object.keys(p).length > 0 ? p : undefined
  }, [debouncedSearch, filterWithoutSupplier, filterSupplierId])

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError("")
      try {
        const data = await getProductsMaster(queryParams)
        if (!cancelled) setRows(data)
      } catch {
        if (!cancelled) {
          setError("No se pudieron cargar los productos")
          setRows([])
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [queryParams])

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

  const handleSupplierChange = useCallback(
    async (row: ProductMasterRow, value: string) => {
      const nextId: number | null =
        value === NONE_SELECT ? null : Number.parseInt(value, 10)
      if (value !== NONE_SELECT && !Number.isFinite(nextId)) return

      const prevId = row.supplier_id
      if (prevId === nextId) return

      setRowErrors((e) => {
        const copy = { ...e }
        delete copy[row.barcode]
        return copy
      })
      clearSaveSuccessFlash(row.barcode)
      setPending(row.barcode, true)
      setRows((rs) =>
        rs.map((r) =>
          r.barcode === row.barcode ? { ...r, supplier_id: nextId } : r,
        ),
      )
      try {
        await patchProductMaster(row.barcode, {
          supplier_id: nextId,
        })
        const refreshed = await getProductsMaster(queryParams)
        setRows(refreshed)
        void loadUnassignedCount()
        flashSaveSuccess(row.barcode)
      } catch {
        setRows((rs) =>
          rs.map((r) =>
            r.barcode === row.barcode ? { ...r, supplier_id: prevId } : r,
          ),
        )
        setRowErrors((e) => ({
          ...e,
          [row.barcode]: "No se pudo guardar",
        }))
      } finally {
        setPending(row.barcode, false)
      }
    },
    [
      queryParams,
      setPending,
      loadUnassignedCount,
      clearSaveSuccessFlash,
      flashSaveSuccess,
    ],
  )

  const selectDisabled = suppliersLoading || !!suppliersError || suppliers.length === 0

  const onFilterWithoutSupplierChange = (checked: boolean) => {
    setFilterWithoutSupplier(checked)
    if (checked) setFilterSupplierId(FILTER_ALL_SUPPLIERS)
  }

  const onFilterSupplierChange = (value: string) => {
    setFilterSupplierId(value)
    if (value !== FILTER_ALL_SUPPLIERS) setFilterWithoutSupplier(false)
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Productos por proveedor</h1>
          <p className="text-sm text-muted-foreground">
            Productos del catálogo maestro y su proveedor asignado
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          className="shrink-0"
          onClick={() => {
            setBulkFileName(null)
            if (bulkFileInputRef.current) bulkFileInputRef.current.value = ""
            setBulkDialogOpen(true)
          }}
        >
          Carga masiva
        </Button>
      </div>

      <Dialog open={bulkDialogOpen} onOpenChange={setBulkDialogOpen}>
        <DialogContent className="max-h-[min(90vh,32rem)] overflow-y-auto sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Carga masiva de proveedor</DialogTitle>
            <DialogDescription>
              Archivo de texto con una fila por producto. El backend aún no procesa este archivo;
              solo puedes revisar el formato y elegir un archivo.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3 text-sm">
            <p className="font-medium text-foreground">Formato (separador: barra vertical)</p>
            <code className="text-muted-foreground">barcode | supplier_id</code>
            <p className="text-muted-foreground">Ejemplo:</p>
            <pre
              className="overflow-x-auto rounded-md border border-border bg-muted/50 p-3 font-mono text-xs leading-relaxed text-foreground"
              tabIndex={0}
            >
              {`7801234567890 | 3
7809876543210 | 2`}
            </pre>
          </div>

          <input
            ref={bulkFileInputRef}
            type="file"
            className="sr-only"
            accept=".txt,.csv,text/plain"
            onChange={(e) => {
              const f = e.target.files?.[0]
              setBulkFileName(f?.name ?? null)
            }}
          />

          <div className="flex flex-col gap-2">
            <Button
              type="button"
              variant="secondary"
              className="w-full sm:w-auto"
              onClick={() => bulkFileInputRef.current?.click()}
            >
              <Upload className="size-4" aria-hidden />
              Subir archivo
            </Button>
            {bulkFileName ? (
              <p className="text-xs text-muted-foreground">
                Seleccionado: <span className="font-medium text-foreground">{bulkFileName}</span>
              </p>
            ) : null}
          </div>

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cerrar
              </Button>
            </DialogClose>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <div className="flex flex-wrap items-stretch gap-3">
        <Card className="border-amber-500/40 bg-amber-500/[0.07] shadow-none">
          <CardContent className="flex items-center gap-3 py-3 pl-4 pr-4">
            <div className="flex size-9 shrink-0 items-center justify-center rounded-md bg-amber-500/15 text-amber-700 dark:text-amber-400">
              <PackageMinus className="size-4" aria-hidden />
            </div>
            <div className="min-w-0">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Pendiente de compras
              </p>
              <p className="text-sm text-foreground">
                <span className="font-medium">Productos sin proveedor:</span>{" "}
                {unassignedLoading ? (
                  <span className="inline-flex items-center gap-1.5 text-muted-foreground">
                    <Loader2 className="size-3.5 animate-spin" aria-hidden />
                    …
                  </span>
                ) : unassignedError ? (
                  <span className="text-destructive">{unassignedError}</span>
                ) : (
                  <Badge variant="secondary" className="ml-1 align-middle text-sm font-semibold tabular-nums">
                    {unassignedCount ?? "—"}
                  </Badge>
                )}
              </p>
            </div>
          </CardContent>
        </Card>
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

      <Card>
        <CardHeader>
          <CardTitle>Listado de productos</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-4 border-b border-border pb-4 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="flex items-center gap-2">
              <Checkbox
                id="filter-without-supplier"
                checked={filterWithoutSupplier}
                onCheckedChange={(v) => onFilterWithoutSupplierChange(v === true)}
              />
              <Label
                htmlFor="filter-without-supplier"
                className="cursor-pointer text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
              >
                Sin proveedor
              </Label>
            </div>

            <div className="flex min-w-[200px] flex-1 flex-col gap-1.5 sm:max-w-xs">
              <Label className="text-xs text-muted-foreground">Por proveedor</Label>
              <Select
                value={filterSupplierId}
                disabled={filterWithoutSupplier || suppliersLoading || !!suppliersError}
                onValueChange={onFilterSupplierChange}
              >
                <SelectTrigger className="h-9 w-full">
                  <SelectValue placeholder="Todos los proveedores" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={FILTER_ALL_SUPPLIERS}>Todos los proveedores</SelectItem>
                  {suppliers.map((s) => (
                    <SelectItem key={s.id} value={String(s.id)}>
                      {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex min-w-[220px] flex-1 flex-col gap-1.5 sm:max-w-md">
              <Label htmlFor="product-search" className="text-xs text-muted-foreground">
                Buscar por nombre o código de barras
              </Label>
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="product-search"
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
            <table className="w-full">
              <thead>
                <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                  <th className="pb-3">Barcode</th>
                  <th className="pb-3">Producto</th>
                  <th className="pb-3">Variante</th>
                  <th className="pb-3">Proveedor</th>
                  <th className="pb-3">Tipo</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={5} className="py-6 text-center text-sm text-muted-foreground">
                      Cargando productos...
                    </td>
                  </tr>
                ) : rows.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="py-6 text-center text-sm text-muted-foreground">
                      No hay productos para mostrar
                    </td>
                  </tr>
                ) : (
                  rows.map((row) => {
                    const pending = pendingBarcodes.has(row.barcode)
                    const showSaved = saveSuccessBarcodes.has(row.barcode)
                    const currentValue =
                      row.supplier_id != null ? String(row.supplier_id) : NONE_SELECT
                    const orphanSupplier =
                      row.supplier_id != null &&
                      !suppliers.some((s) => s.id === row.supplier_id)

                    return (
                      <tr
                        key={`${row.id}-${row.barcode}`}
                        className={cn(
                          "group border-b border-border transition-colors duration-150 last:border-0",
                          "hover:bg-muted/70",
                          pending && "bg-muted/40",
                        )}
                      >
                        <td className="py-3 align-middle font-mono text-xs">{row.barcode}</td>
                        <td className="py-3 align-middle font-medium">
                          {row.product_name || "—"}
                        </td>
                        <td className="py-3 align-middle">{row.variant_name || "—"}</td>
                        <td className="py-3 align-middle">
                          <div
                            className={cn(
                              "flex min-w-[200px] max-w-[280px] items-center gap-2 rounded-md border border-transparent px-0.5 py-0.5 transition-[border-color,background-color]",
                              "group-hover:border-dashed group-hover:border-border group-hover:bg-accent/25",
                            )}
                          >
                            <Select
                              value={currentValue}
                              disabled={selectDisabled || pending}
                              onValueChange={(v) => handleSupplierChange(row, v)}
                            >
                              <SelectTrigger className="h-8 w-full min-w-0 flex-1 shadow-xs group-hover:bg-background/90 dark:group-hover:bg-background/70">
                                <SelectValue placeholder="Elegir proveedor…" />
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
                            <span
                              className="flex size-7 shrink-0 items-center justify-center"
                              aria-live="polite"
                            >
                              {pending ? (
                                <Loader2
                                  className="size-4 animate-spin text-muted-foreground"
                                  aria-label="Guardando"
                                />
                              ) : showSaved ? (
                                <Check
                                  className="size-4 text-emerald-600 dark:text-emerald-400"
                                  strokeWidth={2.5}
                                  aria-label="Guardado"
                                />
                              ) : null}
                            </span>
                          </div>
                          {rowErrors[row.barcode] ? (
                            <p className="mt-1 text-xs text-destructive">
                              {rowErrors[row.barcode]}
                            </p>
                          ) : null}
                        </td>
                        <td className="py-3 align-middle">{row.product_type || "—"}</td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
