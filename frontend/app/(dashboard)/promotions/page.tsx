"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Checkbox } from "@/components/ui/checkbox"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  createPromotion,
  getCompanies,
  getProductsMaster,
  getPromotionsGrid,
  patchPromotionSnapshotSalePrice,
  type Company,
  type CreatePromotionPayload,
  type ProductMasterRow,
  type PromotionGridRow,
} from "@/lib/api"
import { parsePrice, parsePriceInput } from "@/lib/promotions-utils"
import { PromotionActiveGrid } from "@/components/promotions/promotion-active-grid"
import { PromotionAdminTable } from "@/components/promotions/promotion-admin-table"
import { PromotionCalendarView } from "@/components/promotions/promotion-calendar-view"
import { PromotionDetailSheet } from "@/components/promotions/promotion-detail-sheet"
import { PromotionFilters } from "@/components/promotions/promotion-filters"
import {
  CalendarDays,
  FileSpreadsheet,
  LayoutGrid,
  Loader2,
  Plus,
  RefreshCw,
  Search,
  Table2,
  Trash2,
  UserPlus,
} from "lucide-react"

type ProductLine = {
  id: string
  barcode: string
  tipo_descuento: "porcentaje" | "precio_fijo"
  valor: string
  observacion: string
}

type CreateMode = "choose" | "individual" | "bulk"

function newLineId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`
}

function emptyLine(): ProductLine {
  return {
    id: newLineId(),
    barcode: "",
    tipo_descuento: "porcentaje",
    valor: "",
    observacion: "",
  }
}

export default function PromotionsPage() {
  const [rows, setRows] = useState<PromotionGridRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState("activas")

  const [filterTipo, setFilterTipo] = useState("all")
  const [filterEstado, setFilterEstado] = useState("Activa")
  const [filterCompanyId, setFilterCompanyId] = useState("all")

  const [companies, setCompanies] = useState<Company[]>([])
  const [createWarnings, setCreateWarnings] = useState<string[]>([])

  const [createOpen, setCreateOpen] = useState(false)
  const [createMode, setCreateMode] = useState<CreateMode>("choose")
  const [createSubmitting, setCreateSubmitting] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  const [formTipo, setFormTipo] = useState<"oferta" | "remate">("oferta")
  const [formCanal, setFormCanal] = useState<"ruta" | "detalle">("detalle")
  const [formInicio, setFormInicio] = useState("")
  const [formFin, setFormFin] = useState("")
  const [formLines, setFormLines] = useState<ProductLine[]>([emptyLine()])
  const [formRutaPriceList, setFormRutaPriceList] = useState("")
  const [selectedCompanyIds, setSelectedCompanyIds] = useState<Set<number>>(new Set())
  const [formSharedPriceList, setFormSharedPriceList] = useState("")

  const [searchOpen, setSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState("")
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchResults, setSearchResults] = useState<ProductMasterRow[]>([])
  const [searchTargetLineId, setSearchTargetLineId] = useState<string | null>(null)

  const [detailRow, setDetailRow] = useState<PromotionGridRow | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)

  const [editPriceOpen, setEditPriceOpen] = useState(false)
  const [editPriceRow, setEditPriceRow] = useState<PromotionGridRow | null>(null)
  const [editSalePriceInput, setEditSalePriceInput] = useState("")
  const [editPriceSubmitting, setEditPriceSubmitting] = useState(false)
  const [editPriceError, setEditPriceError] = useState<string | null>(null)

  const companyNameById = useMemo(() => {
    const m = new Map<number, string>()
    for (const c of companies) m.set(c.company_id, c.name)
    return m
  }, [companies])

  const gridEstadoFilter = useMemo(() => {
    if (activeTab === "activas" && filterEstado === "all") return "Activa"
    if (filterEstado === "all") return undefined
    return filterEstado
  }, [activeTab, filterEstado])

  const loadGrid = useCallback(async () => {
    setError(null)
    setLoading(true)
    try {
      const data = await getPromotionsGrid({
        tipo: filterTipo === "all" ? undefined : filterTipo,
        estado: gridEstadoFilter,
        company_id:
          filterCompanyId === "all" ? undefined : parseInt(filterCompanyId, 10),
      })
      setRows(data)
    } catch (e) {
      setRows([])
      setError(e instanceof Error ? e.message : "Error al cargar datos")
    } finally {
      setLoading(false)
    }
  }, [filterTipo, gridEstadoFilter, filterCompanyId])

  useEffect(() => {
    void loadGrid()
  }, [loadGrid])

  useEffect(() => {
    let cancelled = false
    void (async () => {
      try {
        const list = await getCompanies()
        if (!cancelled) setCompanies(list)
      } catch {
        if (!cancelled) setCompanies([])
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  const resetCreateForm = () => {
    setFormTipo("oferta")
    setFormCanal("detalle")
    setFormInicio("")
    setFormFin("")
    setFormLines([emptyLine()])
    setFormRutaPriceList("")
    setSelectedCompanyIds(new Set())
    setFormSharedPriceList("")
    setCreateError(null)
    setCreateMode("choose")
  }

  const openCreate = () => {
    resetCreateForm()
    setCreateOpen(true)
  }

  const openDuplicate = (row: PromotionGridRow) => {
    resetCreateForm()
    setCreateMode("individual")
    setFormTipo(row.tipo as "oferta" | "remate")
    setFormCanal(row.canal as "ruta" | "detalle")
    setFormInicio(row.fecha_inicio?.slice(0, 10) ?? "")
    setFormFin(row.fecha_fin?.slice(0, 10) ?? "")
    const sale = parsePrice(row.sale_price)
    setFormLines([
      {
        id: newLineId(),
        barcode: row.codigo_barras,
        tipo_descuento: "precio_fijo",
        valor: sale != null ? String(Math.round(sale)) : "",
        observacion: row.observacion ?? "",
      },
    ])
    setSelectedCompanyIds(new Set([row.company_id]))
    if (row.price_list) setFormSharedPriceList(row.price_list)
    setCreateOpen(true)
  }

  const runProductSearch = async () => {
    const q = searchQuery.trim()
    if (!q) {
      setSearchResults([])
      return
    }
    setSearchLoading(true)
    try {
      const page = await getProductsMaster({ search: q, limit: 30, offset: 0 })
      setSearchResults(page.items)
    } catch {
      setSearchResults([])
    } finally {
      setSearchLoading(false)
    }
  }

  const pickSearchResult = (item: ProductMasterRow) => {
    const bc = (item.barcode || "").trim()
    if (!bc) return
    setFormLines((prev) => {
      if (searchTargetLineId) {
        return prev.map((l) =>
          l.id === searchTargetLineId ? { ...l, barcode: bc } : l,
        )
      }
      return [...prev, { ...emptyLine(), id: newLineId(), barcode: bc }]
    })
    setSearchOpen(false)
    setSearchQuery("")
    setSearchResults([])
    setSearchTargetLineId(null)
  }

  const submitCreate = async () => {
    setCreateError(null)
    const items = formLines
      .map((l) => ({
        barcode: l.barcode.trim(),
        tipo_descuento: l.tipo_descuento,
        valor: parseFloat(l.valor.replace(",", ".")),
        observacion: l.observacion.trim() || null,
      }))
      .filter((l) => l.barcode.length > 0)

    if (!formInicio || !formFin) {
      setCreateError("Indique fecha de inicio y fin.")
      return
    }
    if (items.length === 0) {
      setCreateError("Agregue al menos un producto con código de barras.")
      return
    }
    for (const it of items) {
      if (!Number.isFinite(it.valor) || it.valor < 0) {
        setCreateError(`Valor inválido para el código ${it.barcode}.`)
        return
      }
    }

    let companiesPayload: CreatePromotionPayload["companies"] = []
    if (formCanal === "detalle") {
      if (selectedCompanyIds.size === 0) {
        setCreateError("Seleccione al menos una empresa.")
        return
      }
      const pl = formSharedPriceList.trim() || null
      companiesPayload = [...selectedCompanyIds].map((company_id) => ({
        company_id,
        price_list: pl,
      }))
    } else {
      const pl = formRutaPriceList.trim() || null
      if (pl) companiesPayload = [{ company_id: 3, price_list: pl }]
    }

    const payload: CreatePromotionPayload = {
      tipo: formTipo,
      canal: formCanal,
      fecha_inicio: formInicio,
      fecha_fin: formFin,
      activa: true,
      items: items.map((it) => ({
        barcode: it.barcode,
        tipo_descuento: it.tipo_descuento,
        valor: it.valor,
        observacion: it.observacion,
      })),
      companies: companiesPayload,
    }

    setCreateSubmitting(true)
    try {
      const result = await createPromotion(payload)
      setCreateWarnings(result.warnings ?? [])
      setCreateOpen(false)
      resetCreateForm()
      await loadGrid()
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setCreateSubmitting(false)
    }
  }

  const openDetail = (row: PromotionGridRow) => {
    setDetailRow(row)
    setDetailOpen(true)
  }

  const openEditSalePrice = (row: PromotionGridRow) => {
    setEditPriceRow(row)
    const current = parsePrice(row.sale_price)
    setEditSalePriceInput(current != null ? String(Math.round(current)) : "")
    setEditPriceError(null)
    setEditPriceOpen(true)
  }

  const submitEditSalePrice = async () => {
    if (!editPriceRow) return
    const sale = parsePriceInput(editSalePriceInput)
    if (sale == null) {
      setEditPriceError("Ingrese un precio promocional válido.")
      return
    }
    setEditPriceSubmitting(true)
    setEditPriceError(null)
    try {
      await patchPromotionSnapshotSalePrice(editPriceRow.snapshot_id, sale)
      setEditPriceOpen(false)
      setEditPriceRow(null)
      await loadGrid()
    } catch (e) {
      setEditPriceError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setEditPriceSubmitting(false)
    }
  }

  const handleTabChange = (tab: string) => {
    setActiveTab(tab)
    if (tab === "activas") {
      setFilterEstado("Activa")
    } else {
      setFilterEstado("all")
    }
  }

  const activeCount = useMemo(
    () => rows.filter((r) => r.estado === "Activa").length,
    [rows],
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Promociones</h1>
          <p className="text-muted-foreground mt-1 text-sm">
            Ofertas y remates para sucursales · precios congelados listos para etiquetas
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" onClick={() => void loadGrid()} disabled={loading}>
            <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Actualizar
          </Button>
          <Button size="sm" onClick={openCreate}>
            <Plus className="mr-2 h-4 w-4" />
            Crear promoción
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {createWarnings.length > 0 ? (
        <Alert>
          <AlertTitle>Advertencias al crear promoción</AlertTitle>
          <AlertDescription>
            <ul className="mt-1 list-inside list-disc text-sm">
              {createWarnings.map((w) => (
                <li key={w}>{w}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      ) : null}

      <Tabs value={activeTab} onValueChange={handleTabChange} className="space-y-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <TabsList className="h-auto flex-wrap">
            <TabsTrigger value="activas" className="gap-1.5">
              <LayoutGrid className="h-4 w-4" />
              Activas
              {!loading && activeTab === "activas" ? (
                <span className="bg-emerald-100 text-emerald-800 ml-1 rounded-full px-1.5 text-xs">
                  {activeCount}
                </span>
              ) : null}
            </TabsTrigger>
            <TabsTrigger value="calendario" className="gap-1.5">
              <CalendarDays className="h-4 w-4" />
              Calendario
            </TabsTrigger>
            <TabsTrigger value="admin" className="gap-1.5">
              <Table2 className="h-4 w-4" />
              Administrativa
            </TabsTrigger>
          </TabsList>

          <PromotionFilters
            filterTipo={filterTipo}
            filterEstado={filterEstado}
            filterCompanyId={filterCompanyId}
            companies={companies}
            onTipoChange={setFilterTipo}
            onEstadoChange={setFilterEstado}
            onCompanyChange={setFilterCompanyId}
            showEstado={activeTab !== "activas"}
          />
        </div>

        <TabsContent value="activas" className="mt-0">
          <PromotionActiveGrid
            rows={rows}
            loading={loading}
            companyNameById={companyNameById}
            onOpen={openDetail}
            onEdit={openEditSalePrice}
            onDuplicate={openDuplicate}
          />
        </TabsContent>

        <TabsContent value="calendario" className="mt-0">
          <PromotionCalendarView
            rows={rows}
            loading={loading}
            companyNameById={companyNameById}
            onOpen={openDetail}
          />
        </TabsContent>

        <TabsContent value="admin" className="mt-0">
          <PromotionAdminTable
            rows={rows}
            loading={loading}
            companyNameById={companyNameById}
            onOpen={openDetail}
          />
        </TabsContent>
      </Tabs>

      <PromotionDetailSheet
        row={detailRow}
        open={detailOpen}
        companyName={
          detailRow
            ? companyNameById.get(detailRow.company_id) ?? `Empresa ${detailRow.company_id}`
            : ""
        }
        onOpenChange={setDetailOpen}
        onEditSalePrice={openEditSalePrice}
      />

      {/* Crear promoción */}
      <Dialog
        open={createOpen}
        onOpenChange={(o) => {
          setCreateOpen(o)
          if (!o) resetCreateForm()
        }}
      >
        <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
          {createMode === "choose" ? (
            <>
              <DialogHeader>
                <DialogTitle>Crear promoción</DialogTitle>
                <DialogDescription>
                  Elija cómo desea cargar productos en promoción.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-3 py-4 sm:grid-cols-2">
                <button
                  type="button"
                  className="hover:border-primary hover:bg-muted/40 flex flex-col items-start gap-2 rounded-xl border p-4 text-left transition-colors"
                  onClick={() => setCreateMode("individual")}
                >
                  <UserPlus className="text-primary h-8 w-8" />
                  <span className="font-semibold">Individual</span>
                  <span className="text-muted-foreground text-sm">
                    Uno o pocos productos con precio congelado al crear.
                  </span>
                </button>
                <button
                  type="button"
                  className="flex flex-col items-start gap-2 rounded-xl border border-dashed p-4 text-left opacity-70"
                  disabled
                  title="Próximamente"
                >
                  <FileSpreadsheet className="h-8 w-8" />
                  <span className="font-semibold">Carga masiva Excel</span>
                  <span className="text-muted-foreground text-sm">
                    Campañas con muchos productos (ej. OFERTA CCU JULIO). Próximamente.
                  </span>
                </button>
              </div>
            </>
          ) : (
            <>
              <DialogHeader>
                <DialogTitle>Nueva promoción</DialogTitle>
                <DialogDescription>
                  El precio ANTES se congela al guardar. La lista se asigna automáticamente por
                  empresa si no la indica.
                </DialogDescription>
              </DialogHeader>

              {createError ? (
                <Alert variant="destructive">
                  <AlertDescription>{createError}</AlertDescription>
                </Alert>
              ) : null}

              <div className="grid gap-4 py-2">
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <div className="grid gap-2">
                    <Label>Tipo</Label>
                    <Select
                      value={formTipo}
                      onValueChange={(v) => setFormTipo(v as "oferta" | "remate")}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="oferta">Oferta</SelectItem>
                        <SelectItem value="remate">Remate</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid gap-2">
                    <Label>Canal</Label>
                    <Select
                      value={formCanal}
                      onValueChange={(v) => setFormCanal(v as "ruta" | "detalle")}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="detalle">Detalle</SelectItem>
                        <SelectItem value="ruta">Ruta</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <div className="grid gap-2">
                    <Label>Fecha inicio</Label>
                    <Input
                      type="date"
                      value={formInicio}
                      onChange={(e) => setFormInicio(e.target.value)}
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label>Fecha fin</Label>
                    <Input
                      type="date"
                      value={formFin}
                      onChange={(e) => setFormFin(e.target.value)}
                    />
                  </div>
                </div>

                {formCanal === "detalle" ? (
                  <>
                    <div className="grid gap-2">
                      <Label>Empresas</Label>
                      <ScrollArea className="h-36 rounded-md border p-3">
                        <div className="space-y-2">
                          {companies.map((c) => (
                            <label
                              key={c.company_id}
                              className="flex cursor-pointer items-center gap-2 text-sm"
                            >
                              <Checkbox
                                checked={selectedCompanyIds.has(c.company_id)}
                                onCheckedChange={(chk) => {
                                  setSelectedCompanyIds((prev) => {
                                    const next = new Set(prev)
                                    if (chk === true) next.add(c.company_id)
                                    else next.delete(c.company_id)
                                    return next
                                  })
                                }}
                              />
                              <span>{c.name}</span>
                            </label>
                          ))}
                        </div>
                      </ScrollArea>
                    </div>
                    <div className="grid gap-2">
                      <Label>Lista de precio (opcional)</Label>
                      <Input
                        placeholder="Auto por empresa si se deja vacío"
                        value={formSharedPriceList}
                        onChange={(e) => setFormSharedPriceList(e.target.value)}
                      />
                    </div>
                  </>
                ) : (
                  <div className="grid gap-2">
                    <Label>Lista de precio ruta (opcional)</Label>
                    <Input
                      value={formRutaPriceList}
                      onChange={(e) => setFormRutaPriceList(e.target.value)}
                    />
                  </div>
                )}

                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <Label>Productos</Label>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => setFormLines((p) => [...p, emptyLine()])}
                    >
                      <Plus className="mr-1 h-4 w-4" />
                      Línea
                    </Button>
                  </div>
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Código</TableHead>
                          <TableHead>Descuento</TableHead>
                          <TableHead>Valor</TableHead>
                          <TableHead className="w-[88px]" />
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {formLines.map((line) => (
                          <TableRow key={line.id}>
                            <TableCell>
                              <Input
                                className="font-mono text-xs"
                                value={line.barcode}
                                onChange={(e) =>
                                  setFormLines((prev) =>
                                    prev.map((l) =>
                                      l.id === line.id ? { ...l, barcode: e.target.value } : l,
                                    ),
                                  )
                                }
                              />
                            </TableCell>
                            <TableCell>
                              <Select
                                value={line.tipo_descuento}
                                onValueChange={(v) =>
                                  setFormLines((prev) =>
                                    prev.map((l) =>
                                      l.id === line.id
                                        ? {
                                            ...l,
                                            tipo_descuento: v as "porcentaje" | "precio_fijo",
                                          }
                                        : l,
                                    ),
                                  )
                                }
                              >
                                <SelectTrigger className="w-[120px]">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="porcentaje">%</SelectItem>
                                  <SelectItem value="precio_fijo">Fijo</SelectItem>
                                </SelectContent>
                              </Select>
                            </TableCell>
                            <TableCell>
                              <Input
                                className="w-24"
                                value={line.valor}
                                onChange={(e) =>
                                  setFormLines((prev) =>
                                    prev.map((l) =>
                                      l.id === line.id ? { ...l, valor: e.target.value } : l,
                                    ),
                                  )
                                }
                              />
                            </TableCell>
                            <TableCell>
                              <div className="flex gap-1">
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-8 w-8"
                                  onClick={() => {
                                    setSearchTargetLineId(line.id)
                                    setSearchOpen(true)
                                  }}
                                >
                                  <Search className="h-4 w-4" />
                                </Button>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-8 w-8 text-destructive"
                                  disabled={formLines.length <= 1}
                                  onClick={() =>
                                    setFormLines((prev) =>
                                      prev.length <= 1
                                        ? prev
                                        : prev.filter((l) => l.id !== line.id),
                                    )
                                  }
                                >
                                  <Trash2 className="h-4 w-4" />
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              </div>

              <DialogFooter>
                <Button variant="outline" type="button" onClick={() => setCreateMode("choose")}>
                  Volver
                </Button>
                <Button onClick={() => void submitCreate()} disabled={createSubmitting}>
                  {createSubmitting ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Guardando…
                    </>
                  ) : (
                    "Guardar promoción"
                  )}
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={searchOpen} onOpenChange={setSearchOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Buscar producto</DialogTitle>
          </DialogHeader>
          <div className="flex gap-2">
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void runProductSearch()
              }}
            />
            <Button type="button" onClick={() => void runProductSearch()} disabled={searchLoading}>
              {searchLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            </Button>
          </div>
          <ScrollArea className="h-64 rounded-md border">
            <ul className="divide-y p-2">
              {searchResults.map((it) => (
                <li key={it.id}>
                  <button
                    type="button"
                    className="hover:bg-muted flex w-full flex-col items-start rounded px-2 py-2 text-left text-sm"
                    onClick={() => pickSearchResult(it)}
                  >
                    <span className="font-medium">{it.product_name || "—"}</span>
                    <span className="text-muted-foreground text-xs">{it.barcode}</span>
                  </button>
                </li>
              ))}
            </ul>
          </ScrollArea>
        </DialogContent>
      </Dialog>

      <Dialog open={editPriceOpen} onOpenChange={setEditPriceOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Editar precio AHORA</DialogTitle>
            <DialogDescription>
              El precio ANTES permanece congelado desde la creación.
            </DialogDescription>
          </DialogHeader>
          {editPriceRow ? (
            <div className="space-y-4 py-2">
              <div className="grid gap-2">
                <Label>ANTES (congelado)</Label>
                <p className="text-muted-foreground text-lg line-through">
                  {new Intl.NumberFormat("es-CL", {
                    style: "currency",
                    currency: "CLP",
                    maximumFractionDigits: 0,
                  }).format(parsePrice(editPriceRow.regular_price) ?? 0)}
                </p>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="edit-sale-price">AHORA</Label>
                <Input
                  id="edit-sale-price"
                  inputMode="numeric"
                  value={editSalePriceInput}
                  onChange={(e) => setEditSalePriceInput(e.target.value)}
                />
              </div>
              {editPriceError ? (
                <Alert variant="destructive">
                  <AlertDescription>{editPriceError}</AlertDescription>
                </Alert>
              ) : null}
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditPriceOpen(false)}>
              Cancelar
            </Button>
            <Button disabled={editPriceSubmitting} onClick={() => void submitEditSalePrice()}>
              {editPriceSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : "Guardar"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
