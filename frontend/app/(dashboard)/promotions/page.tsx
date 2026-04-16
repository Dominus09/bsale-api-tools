"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Checkbox } from "@/components/ui/checkbox"
import { Switch } from "@/components/ui/switch"
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
import {
  createPromotion,
  getCompanies,
  getProductsMaster,
  getPromotionsGrid,
  togglePromotion,
  type Company,
  type CreatePromotionPayload,
  type ProductMasterRow,
  type PromotionGridRow,
} from "@/lib/api"
import { Loader2, Plus, RefreshCw, Search, Trash2 } from "lucide-react"

const ESTADOS = ["Activa", "Vencida", "Programada", "Inactiva"] as const

type ProductLine = {
  id: string
  barcode: string
  tipo_descuento: "porcentaje" | "precio_fijo"
  valor: string
  observacion: string
}

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

function estadoBadgeClass(estado: string) {
  switch (estado) {
    case "Activa":
      return "border-transparent bg-emerald-600 text-white hover:bg-emerald-600/90"
    case "Vencida":
      return "border-transparent bg-red-600 text-white hover:bg-red-600/90"
    case "Programada":
      return "border-transparent bg-amber-500 text-amber-950 hover:bg-amber-500/90"
    case "Inactiva":
    default:
      return "border-transparent bg-zinc-500 text-white hover:bg-zinc-500/90"
  }
}

function formatMoney(n: number | string | null | undefined) {
  const v = typeof n === "string" ? parseFloat(n) : Number(n)
  if (!Number.isFinite(v)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(v)
}

function formatDate(s: string | null | undefined) {
  if (!s) return "—"
  const d = s.slice(0, 10)
  return d || "—"
}

export default function PromotionsPage() {
  const [rows, setRows] = useState<PromotionGridRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [filterCanal, setFilterCanal] = useState<string>("all")
  const [filterTipo, setFilterTipo] = useState<string>("all")
  const [filterEstado, setFilterEstado] = useState<string>("all")
  const [filterCompanyId, setFilterCompanyId] = useState<string>("all")

  const [companies, setCompanies] = useState<Company[]>([])
  const [togglingId, setTogglingId] = useState<number | null>(null)

  const [createOpen, setCreateOpen] = useState(false)
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

  const companyNameById = useMemo(() => {
    const m = new Map<number, string>()
    for (const c of companies) {
      m.set(c.company_id, c.name)
    }
    return m
  }, [companies])

  const loadGrid = useCallback(async () => {
    setError(null)
    setLoading(true)
    try {
      const data = await getPromotionsGrid({
        canal: filterCanal === "all" ? undefined : filterCanal,
        tipo: filterTipo === "all" ? undefined : filterTipo,
        estado: filterEstado === "all" ? undefined : filterEstado,
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
  }, [filterCanal, filterTipo, filterEstado, filterCompanyId])

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
  }

  const openCreate = () => {
    resetCreateForm()
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
        setCreateError("Canal detalle requiere al menos una empresa.")
        return
      }
      const pl = formSharedPriceList.trim() || null
      companiesPayload = [...selectedCompanyIds].map((company_id) => ({
        company_id,
        price_list: pl,
      }))
    } else {
      const pl = formRutaPriceList.trim() || null
      if (pl) {
        companiesPayload = [{ company_id: 3, price_list: pl }]
      }
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
      await createPromotion(payload)
      setCreateOpen(false)
      resetCreateForm()
      await loadGrid()
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setCreateSubmitting(false)
    }
  }

  const onToggleRow = async (promotionId: number) => {
    setTogglingId(promotionId)
    setError(null)
    try {
      await togglePromotion(promotionId)
      await loadGrid()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cambiar estado")
    } finally {
      setTogglingId(null)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Promociones</h1>
          <p className="text-muted-foreground text-sm">
            Ofertas y remates (grilla desde el backend).
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

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Filtros</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-4">
          <div className="grid w-full min-w-[140px] max-w-xs gap-2">
            <Label>Canal</Label>
            <Select value={filterCanal} onValueChange={setFilterCanal}>
              <SelectTrigger>
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos</SelectItem>
                <SelectItem value="ruta">Ruta</SelectItem>
                <SelectItem value="detalle">Detalle</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="grid w-full min-w-[140px] max-w-xs gap-2">
            <Label>Tipo</Label>
            <Select value={filterTipo} onValueChange={setFilterTipo}>
              <SelectTrigger>
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos</SelectItem>
                <SelectItem value="oferta">Oferta</SelectItem>
                <SelectItem value="remate">Remate</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="grid w-full min-w-[140px] max-w-xs gap-2">
            <Label>Estado</Label>
            <Select value={filterEstado} onValueChange={setFilterEstado}>
              <SelectTrigger>
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos</SelectItem>
                {ESTADOS.map((e) => (
                  <SelectItem key={e} value={e}>
                    {e}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid w-full min-w-[180px] max-w-sm gap-2">
            <Label>Empresa</Label>
            <Select value={filterCompanyId} onValueChange={setFilterCompanyId}>
              <SelectTrigger>
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todas</SelectItem>
                {companies.map((c) => (
                  <SelectItem key={c.company_id} value={String(c.company_id)}>
                    {c.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Listado</CardTitle>
        </CardHeader>
        <CardContent className="relative p-0 sm:p-6">
          {loading ? (
            <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
              <Loader2 className="h-6 w-6 animate-spin" />
              <span>Cargando…</span>
            </div>
          ) : (
            <ScrollArea className="h-[min(70vh,720px)] w-full">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="sticky left-0 z-10 bg-card">Activa</TableHead>
                    <TableHead>Tipo producto</TableHead>
                    <TableHead>Producto</TableHead>
                    <TableHead>Variante</TableHead>
                    <TableHead>Código barras</TableHead>
                    <TableHead>Precio normal</TableHead>
                    <TableHead>Precio oferta</TableHead>
                    <TableHead>Descuento %</TableHead>
                    <TableHead>Inicio</TableHead>
                    <TableHead>Fin</TableHead>
                    <TableHead>Tipo</TableHead>
                    <TableHead>Observación</TableHead>
                    <TableHead>Canal</TableHead>
                    <TableHead>Estado</TableHead>
                    <TableHead>Empresa</TableHead>
                    <TableHead>Lista precio</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={15} className="text-center text-muted-foreground">
                        Sin filas para los filtros actuales.
                      </TableCell>
                    </TableRow>
                  ) : (
                    rows.map((r) => (
                      <TableRow
                        key={`${r.promotion_id}-${r.company_id}-${r.codigo_barras}`}
                      >
                        <TableCell className="sticky left-0 z-10 bg-card">
                          <Switch
                            checked={!!r.activa}
                            disabled={togglingId === r.promotion_id}
                            onCheckedChange={() => void onToggleRow(r.promotion_id)}
                          />
                        </TableCell>
                        <TableCell className="max-w-[120px] truncate">{r.tipo_producto}</TableCell>
                        <TableCell className="max-w-[160px] truncate">{r.producto}</TableCell>
                        <TableCell className="max-w-[120px] truncate">{r.variante}</TableCell>
                        <TableCell className="font-mono text-xs">{r.codigo_barras}</TableCell>
                        <TableCell>{formatMoney(r.precio_normal)}</TableCell>
                        <TableCell>{formatMoney(r.precio_oferta)}</TableCell>
                        <TableCell>
                          {(() => {
                            const d = Number(r.descuento_porcentaje)
                            return Number.isFinite(d) ? d.toFixed(2) : "—"
                          })()}
                          %
                          <span className="text-muted-foreground ml-1 text-xs">
                            ({r.descuento_texto})
                          </span>
                        </TableCell>
                        <TableCell>{formatDate(r.fecha_inicio)}</TableCell>
                        <TableCell>{formatDate(r.fecha_fin)}</TableCell>
                        <TableCell className="capitalize">{r.tipo}</TableCell>
                        <TableCell className="max-w-[140px] truncate text-sm">
                          {r.observacion || "—"}
                        </TableCell>
                        <TableCell className="capitalize">{r.canal}</TableCell>
                        <TableCell>
                          <Badge className={estadoBadgeClass(r.estado)}>{r.estado}</Badge>
                        </TableCell>
                        <TableCell className="max-w-[160px] truncate text-sm">
                          {companyNameById.get(r.company_id) ?? `ID ${r.company_id}`}
                        </TableCell>
                        <TableCell className="max-w-[120px] truncate text-sm">
                          {r.price_list || "—"}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </ScrollArea>
          )}
        </CardContent>
      </Card>

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Nueva promoción</DialogTitle>
            <DialogDescription>
              Los datos se envían al servidor con POST /promotions.
            </DialogDescription>
          </DialogHeader>

          {createError ? (
            <Alert variant="destructive">
              <AlertTitle>No se pudo guardar</AlertTitle>
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
                <Input type="date" value={formFin} onChange={(e) => setFormFin(e.target.value)} />
              </div>
            </div>

            {formCanal === "ruta" ? (
              <div className="grid gap-2">
                <Label>Lista de precio (opcional, canal ruta)</Label>
                <Input
                  placeholder="Nombre lista en Bsale"
                  value={formRutaPriceList}
                  onChange={(e) => setFormRutaPriceList(e.target.value)}
                />
                <p className="text-muted-foreground text-xs">
                  Canal ruta usa empresa fija en servidor; solo se usa la lista si la indicas.
                </p>
              </div>
            ) : (
              <>
                <div className="grid gap-2">
                  <Label>Empresas</Label>
                  <ScrollArea className="h-40 rounded-md border p-3">
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
                  <Label>Lista de precio (opcional, todas las empresas)</Label>
                  <Input
                    placeholder="Nombre lista en Bsale"
                    value={formSharedPriceList}
                    onChange={(e) => setFormSharedPriceList(e.target.value)}
                  />
                </div>
              </>
            )}

            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
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
                      <TableHead>Código barras</TableHead>
                      <TableHead>Tipo descuento</TableHead>
                      <TableHead>Valor</TableHead>
                      <TableHead>Obs.</TableHead>
                      <TableHead className="w-[100px]" />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {formLines.map((line) => (
                      <TableRow key={line.id}>
                        <TableCell>
                          <Input
                            className="min-w-[120px] font-mono text-xs"
                            value={line.barcode}
                            onChange={(e) =>
                              setFormLines((prev) =>
                                prev.map((l) =>
                                  l.id === line.id ? { ...l, barcode: e.target.value } : l,
                                ),
                              )
                            }
                            placeholder="780…"
                          />
                        </TableCell>
                        <TableCell>
                          <Select
                            value={line.tipo_descuento}
                            onValueChange={(v) =>
                              setFormLines((prev) =>
                                prev.map((l) =>
                                  l.id === line.id
                                    ? { ...l, tipo_descuento: v as "porcentaje" | "precio_fijo" }
                                    : l,
                                ),
                              )
                            }
                          >
                            <SelectTrigger className="w-[130px]">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="porcentaje">Porcentaje</SelectItem>
                              <SelectItem value="precio_fijo">Precio fijo</SelectItem>
                            </SelectContent>
                          </Select>
                        </TableCell>
                        <TableCell>
                          <Input
                            className="w-24"
                            inputMode="decimal"
                            value={line.valor}
                            onChange={(e) =>
                              setFormLines((prev) =>
                                prev.map((l) =>
                                  l.id === line.id ? { ...l, valor: e.target.value } : l,
                                ),
                              )
                            }
                            placeholder={line.tipo_descuento === "porcentaje" ? "%" : "$"}
                          />
                        </TableCell>
                        <TableCell>
                          <Input
                            className="min-w-[100px]"
                            value={line.observacion}
                            onChange={(e) =>
                              setFormLines((prev) =>
                                prev.map((l) =>
                                  l.id === line.id ? { ...l, observacion: e.target.value } : l,
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
                              title="Buscar producto"
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
                              title="Quitar línea"
                              disabled={formLines.length <= 1}
                              onClick={() =>
                                setFormLines((prev) =>
                                  prev.length <= 1 ? prev : prev.filter((l) => l.id !== line.id),
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
            <Button variant="outline" onClick={() => setCreateOpen(false)} type="button">
              Cancelar
            </Button>
            <Button onClick={() => void submitCreate()} disabled={createSubmitting}>
              {createSubmitting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Guardando…
                </>
              ) : (
                "Guardar"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={searchOpen} onOpenChange={setSearchOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Buscar producto</DialogTitle>
            <DialogDescription>
              Busca en catálogo maestro y asigna el código de barras a la línea.
            </DialogDescription>
          </DialogHeader>
          <div className="flex gap-2">
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Nombre, SKU o código…"
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
                    className="hover:bg-muted flex w-full flex-col items-start gap-0.5 rounded px-2 py-2 text-left text-sm"
                    onClick={() => pickSearchResult(it)}
                  >
                    <span className="font-medium">{it.product_name || "—"}</span>
                    <span className="text-muted-foreground text-xs">
                      {it.barcode}
                      {it.variant_name ? ` · ${it.variant_name}` : ""}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
            {!searchLoading && searchResults.length === 0 && searchQuery.trim() ? (
              <p className="text-muted-foreground p-4 text-center text-sm">Sin resultados.</p>
            ) : null}
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </div>
  )
}
