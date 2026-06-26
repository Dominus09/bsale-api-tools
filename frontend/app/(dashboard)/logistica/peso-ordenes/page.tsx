"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  CheckCircle2,
  Download,
  Loader2,
  PackageCheck,
  RefreshCw,
  ScanSearch,
  Search,
  Stethoscope,
} from "lucide-react"

import {
  createOrderWeightLogistics,
  getAuthHeaders,
  getOrderWeight,
  getOrderWeightHistory,
  orderWeightExportUrl,
  patchOrderWeightProduct,
  recalculateOrderWeight,
  searchOrderWeights,
  type OrderWeightDetail,
  type OrderWeightHistoryRow,
  type OrderWeightLine,
  type OrderWeightSearchRow,
  type ProductMasterLogisticsPatchBody,
} from "@/lib/api"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
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

const ALL = "__all__"

function formatMoney(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(Number(value))
}

function formatKg(v: number | null | undefined) {
  if (v == null || !Number.isFinite(Number(v))) return "—"
  return `${Number(v).toLocaleString("es-CL", { minimumFractionDigits: 2, maximumFractionDigits: 3 })} kg`
}

function formatPct(v: number | null | undefined) {
  if (v == null || !Number.isFinite(Number(v))) return "—"
  return `${Number(v).toFixed(1)}%`
}

function formatDt(iso: string | null | undefined) {
  if (!iso) return "—"
  try {
    return new Date(iso).toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
  } catch {
    return iso
  }
}

const FUENTE_LABEL: Record<string, string> = {
  erp: "ERP",
  manual: "Manual",
  estimado: "Estimado",
  sin_datos: "Sin datos",
}

function LineStatusIcon({ estado }: { estado: string }) {
  if (estado === "completo") return <span title="Completo">🟢</span>
  if (estado === "manual") return <span title="Manual">🟡</span>
  if (estado === "estimado") return <span title="Estimado">🟠</span>
  return <span title="Sin peso">🔴</span>
}

function SemaforoDot({ semaforo }: { semaforo: string }) {
  const cls =
    semaforo === "verde" || semaforo === "verde_claro"
      ? "bg-emerald-500"
      : semaforo === "amarillo"
        ? "bg-amber-400"
        : semaforo === "naranja"
          ? "bg-orange-500"
          : "bg-red-500"
  return <span className={cn("inline-block size-3 rounded-full", cls)} />
}

function OrderEstadoBadge({ estado }: { estado: string }) {
  const ok = estado === "completa"
  return (
    <Badge variant={ok ? "default" : "secondary"} className={ok ? "bg-emerald-600" : "bg-amber-100 text-amber-900"}>
      {ok ? "Completa" : "Incompleta"}
    </Badge>
  )
}

function KpiTile({ label, value, accent }: { label: string; value: string | number; accent?: string }) {
  return (
    <div className={cn("rounded-lg border bg-card p-3", accent)}>
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
    </div>
  )
}

function OrderListCard({
  row,
  selected,
  onSelect,
}: {
  row: OrderWeightSearchRow
  selected: boolean
  onSelect: () => void
}) {
  const incompleta = row.estado !== "completa"
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        "w-full rounded-lg border p-3 text-left transition-colors hover:bg-muted/40",
        selected && "border-primary bg-primary/5 ring-1 ring-primary/30",
        incompleta && !selected && "border-amber-200/80 bg-amber-50/40 dark:bg-amber-950/10",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="font-mono text-base font-semibold">OC {row.oc}</p>
          <p className="mt-0.5 truncate text-sm font-medium">{row.cliente || "Sin cliente"}</p>
          <p className="text-xs text-muted-foreground">{row.comuna || "—"}</p>
        </div>
        <OrderEstadoBadge estado={row.estado} />
      </div>
      <div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <span className="text-muted-foreground">Monto</span>
        <span className="text-right font-medium">{formatMoney(row.total_amount)}</span>
        <span className="text-muted-foreground">Peso</span>
        <span className="text-right font-medium">{formatKg(row.peso_total_kg ?? undefined)}</span>
        <span className="text-muted-foreground">Cobertura</span>
        <span className="text-right">{formatPct(row.porcentaje_cobertura)}</span>
        <span className="text-muted-foreground">Sin peso</span>
        <span className={cn("text-right font-medium", (row.productos_sin_peso ?? 0) > 0 && "text-amber-700")}>
          {row.productos_sin_peso ?? "—"}
        </span>
      </div>
      <p className="mt-2 text-[10px] text-muted-foreground">
        Últ. cálculo: {formatDt(row.ultimo_calculo)}
      </p>
    </button>
  )
}

export default function PesoOrdenesPage() {
  const [oc, setOc] = useState("")
  const [cliente, setCliente] = useState("")
  const [codigoCliente, setCodigoCliente] = useState("")
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [estado, setEstado] = useState(ALL)
  const [billingFilter, setBillingFilter] = useState<"pendientes" | "facturadas" | "todas">("pendientes")
  const [lineFilter, setLineFilter] = useState(ALL)

  const [searching, setSearching] = useState(false)
  const [searchError, setSearchError] = useState<string | null>(null)
  const [results, setResults] = useState<OrderWeightSearchRow[]>([])

  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [order, setOrder] = useState<OrderWeightDetail | null>(null)
  const [orderLoading, setOrderLoading] = useState(false)
  const [orderError, setOrderError] = useState<string | null>(null)
  const [recalculating, setRecalculating] = useState(false)

  const [editLine, setEditLine] = useState<OrderWeightLine | null>(null)
  const [diagLine, setDiagLine] = useState<OrderWeightLine | null>(null)
  const [detectOpen, setDetectOpen] = useState(false)
  const [saving, setSaving] = useState(false)

  const [editForm, setEditForm] = useState<ProductMasterLogisticsPatchBody>({})
  const [history, setHistory] = useState<OrderWeightHistoryRow[]>([])

  const runSearch = useCallback(async () => {
    setSearching(true)
    setSearchError(null)
    try {
      const rows = await searchOrderWeights({
        oc: oc.trim() ? Number.parseInt(oc.trim(), 10) : undefined,
        cliente: cliente.trim() || undefined,
        codigo_cliente: codigoCliente.trim() || undefined,
        date_from: dateFrom || undefined,
        date_to: dateTo || undefined,
        estado: estado === ALL ? undefined : estado,
        billing_filter: billingFilter,
        limit: 200,
      })
      setResults(rows)
    } catch (e) {
      setSearchError(e instanceof Error ? e.message : "Error en búsqueda")
    } finally {
      setSearching(false)
    }
  }, [oc, cliente, codigoCliente, dateFrom, dateTo, estado, billingFilter])

  const loadOrder = useCallback(async (documentId: number, filter?: string) => {
    setOrderLoading(true)
    setOrderError(null)
    setSelectedId(documentId)
    try {
      const data = await getOrderWeight(documentId, {
        line_filter: filter && filter !== ALL ? filter : "all",
      })
      setOrder(data)
      try {
        const h = await getOrderWeightHistory(documentId)
        setHistory(h)
      } catch {
        setHistory([])
      }
    } catch (e) {
      setOrderError(e instanceof Error ? e.message : "Error al cargar orden")
      setOrder(null)
    } finally {
      setOrderLoading(false)
    }
  }, [])

  useEffect(() => {
    void runSearch()
  }, [runSearch])

  useEffect(() => {
    if (selectedId != null) {
      void loadOrder(selectedId, lineFilter)
    }
  }, [lineFilter, selectedId, loadOrder])

  const filteredLines = useMemo(() => {
    const lines = order?.lines ?? []
    if (lineFilter === ALL) return lines
    return lines.filter((ln) => ln.estado_linea === lineFilter)
  }, [order?.lines, lineFilter])

  const sinPesoLines = useMemo(
    () => (order?.lines ?? []).filter((ln) => ln.estado_linea === "sin_peso" && ln.cantidad_unitaria > 0),
    [order?.lines],
  )

  const openEdit = (line: OrderWeightLine) => {
    setEditLine(line)
    setEditForm({
      units_per_box: line.units_per_box ?? undefined,
      weight_box_kg: line.peso_caja_kg ?? undefined,
    })
  }

  const saveEdit = async () => {
    if (!editLine || !order) return
    setSaving(true)
    try {
      const pmId = editLine.products_master_id
      if (!pmId && editLine.variant_id) {
        const created = await createOrderWeightLogistics(editLine.variant_id, order.document_id)
        if (editForm.units_per_box || editForm.weight_box_kg) {
          const newPmId = Number(created.product?.id)
          if (newPmId) {
            const res = await patchOrderWeightProduct(newPmId, order.document_id, editForm)
            setOrder(res.order)
          } else {
            setOrder(created.order)
          }
        } else {
          setOrder(created.order)
        }
        setEditLine(null)
        setDetectOpen(false)
        return
      }
      if (!pmId) throw new Error("Sin ficha logística")
      const res = await patchOrderWeightProduct(pmId, order.document_id, editForm)
      setOrder(res.order)
      setEditLine(null)
    } catch (e) {
      setOrderError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setSaving(false)
    }
  }

  const handleRecalculate = async () => {
    if (!selectedId) return
    setRecalculating(true)
    try {
      const data = await recalculateOrderWeight(selectedId)
      setOrder(data)
      void runSearch()
    } catch (e) {
      setOrderError(e instanceof Error ? e.message : "Error al recalcular")
    } finally {
      setRecalculating(false)
    }
  }

  const handleExport = async () => {
    if (!selectedId) return
    const res = await fetch(orderWeightExportUrl(selectedId), { headers: getAuthHeaders() })
    if (!res.ok) return
    const blob = await res.blob()
    const a = document.createElement("a")
    a.href = URL.createObjectURL(blob)
    a.download = `oc-peso-${order?.oc ?? selectedId}.csv`
    a.click()
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <div>
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Logística</p>
        <h1 className="flex items-center gap-2 text-2xl font-semibold">
          <PackageCheck className="size-7 text-primary" />
          Peso de Órdenes
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Auditoría logística oficial — planificación, camiones, ORS y picking.
        </p>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Buscador</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3 lg:grid-cols-4">
          <div>
            <Label>OC</Label>
            <Input value={oc} onChange={(e) => setOc(e.target.value)} placeholder="67562" />
          </div>
          <div>
            <Label>Cliente</Label>
            <Input value={cliente} onChange={(e) => setCliente(e.target.value)} />
          </div>
          <div>
            <Label>Código cliente</Label>
            <Input value={codigoCliente} onChange={(e) => setCodigoCliente(e.target.value)} />
          </div>
          <div>
            <Label>Facturación</Label>
            <Select value={billingFilter} onValueChange={(v) => setBillingFilter(v as typeof billingFilter)}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="pendientes">Pendientes</SelectItem>
                <SelectItem value="facturadas">Facturadas</SelectItem>
                <SelectItem value="todas">Todas</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Desde</Label>
            <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </div>
          <div>
            <Label>Hasta</Label>
            <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </div>
          <div>
            <Label>Cobertura</Label>
            <Select value={estado} onValueChange={setEstado}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todas</SelectItem>
                <SelectItem value="completa">Completa</SelectItem>
                <SelectItem value="incompleta">Incompleta</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-end">
            <Button onClick={() => void runSearch()} disabled={searching} className="w-full">
              {searching ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Search className="mr-2 size-4" />}
              Buscar
            </Button>
          </div>
        </CardContent>
      </Card>

      {searchError ? (
        <Alert variant="destructive"><AlertDescription>{searchError}</AlertDescription></Alert>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-12">
        <div className="xl:col-span-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">
                Órdenes {billingFilter === "pendientes" ? "pendientes" : billingFilter}
              </CardTitle>
              <p className="text-xs text-muted-foreground">{results.length} resultados</p>
            </CardHeader>
            <CardContent className="max-h-[calc(100vh-16rem)] space-y-2 overflow-y-auto">
              {results.length === 0 ? (
                <p className="py-8 text-center text-sm text-muted-foreground">Sin órdenes para mostrar.</p>
              ) : (
                results.map((r) => (
                  <OrderListCard
                    key={r.document_id}
                    row={r}
                    selected={selectedId === r.document_id}
                    onSelect={() => void loadOrder(r.document_id)}
                  />
                ))
              )}
            </CardContent>
          </Card>
        </div>

        <div className="flex flex-col gap-4 xl:col-span-8">
          {orderLoading ? (
            <div className="flex items-center justify-center py-20 text-muted-foreground">
              <Loader2 className="mr-2 size-5 animate-spin" /> Cargando orden…
            </div>
          ) : null}

          {orderError ? (
            <Alert variant="destructive"><AlertDescription>{orderError}</AlertDescription></Alert>
          ) : null}

          {order ? (
            <>
              <Card>
                <CardHeader className="flex flex-row flex-wrap items-start justify-between gap-3 pb-2">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      OC {order.oc}
                      <OrderEstadoBadge estado={order.estado} />
                    </CardTitle>
                    <p className="text-sm text-muted-foreground">{order.cliente}</p>
                    <p className="text-xs text-muted-foreground">
                      {order.empresa} · {formatMoney(order.total_amount)} · Últ. cálculo {formatDt(order.ultimo_calculo)}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      variant="secondary"
                      size="sm"
                      disabled={sinPesoLines.length === 0}
                      onClick={() => setDetectOpen(true)}
                    >
                      <ScanSearch className="mr-1 size-4" />
                      Detectar sin peso ({sinPesoLines.length})
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => void handleRecalculate()} disabled={recalculating}>
                      {recalculating ? <Loader2 className="mr-1 size-4 animate-spin" /> : <RefreshCw className="mr-1 size-4" />}
                      Recalcular
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => void handleExport()}>
                      <Download className="mr-1 size-4" /> Exportar
                    </Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
                    <KpiTile label="Productos completos" value={order.productos_completos ?? order.productos_con_peso} />
                    <KpiTile label="Sin peso" value={order.productos_sin_peso} accent="border-amber-200/60" />
                    <KpiTile label="Manuales" value={order.productos_manuales} />
                    <KpiTile label="Estimados" value={order.productos_estimados} />
                    <KpiTile
                      label="Cobertura"
                      value={formatPct(order.porcentaje_cobertura)}
                      accent="border-primary/20"
                    />
                    <div className="rounded-lg border bg-card p-3">
                      <p className="text-xs font-medium text-muted-foreground">Semáforo</p>
                      <div className="mt-2 flex items-center gap-2">
                        <SemaforoDot semaforo={order.semaforo} />
                        <span className="text-sm font-medium">{formatKg(order.peso_total_kg)} total</span>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <div className="flex flex-wrap items-center gap-2">
                <Label className="text-xs">Filtrar líneas</Label>
                <Select value={lineFilter} onValueChange={setLineFilter}>
                  <SelectTrigger className="w-[180px]"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL}>Todas</SelectItem>
                    <SelectItem value="completo">Completas</SelectItem>
                    <SelectItem value="manual">Manuales</SelectItem>
                    <SelectItem value="estimado">Estimadas</SelectItem>
                    <SelectItem value="sin_peso">Sin peso</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <Card>
                <CardContent className="overflow-x-auto p-0 pt-4">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Código</TableHead>
                        <TableHead>Producto</TableHead>
                        <TableHead>Variante</TableHead>
                        <TableHead className="text-right">Cant. unit.</TableHead>
                        <TableHead className="text-right">Cant. cajas</TableHead>
                        <TableHead className="text-right">Peso unit.</TableHead>
                        <TableHead className="text-right">Peso caja</TableHead>
                        <TableHead className="text-right">Peso línea</TableHead>
                        <TableHead className="text-right">% total</TableHead>
                        <TableHead>Fuente</TableHead>
                        <TableHead>Est.</TableHead>
                        <TableHead />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredLines.map((ln) => (
                        <TableRow
                          key={ln.detail_id}
                          className={ln.estado_linea === "sin_peso" ? "bg-destructive/5" : undefined}
                        >
                          <TableCell className="font-mono text-xs">{ln.codigo || "—"}</TableCell>
                          <TableCell className="max-w-[160px] text-sm font-medium">{ln.producto || "—"}</TableCell>
                          <TableCell className="max-w-[140px] text-xs text-muted-foreground">
                            {ln.variante || "—"}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{ln.cantidad_unitaria}</TableCell>
                          <TableCell className="text-right tabular-nums">
                            {ln.cantidad_cajas != null ? ln.cantidad_cajas : "—"}
                          </TableCell>
                          <TableCell className="text-right text-xs">{formatKg(ln.peso_unitario_kg)}</TableCell>
                          <TableCell className="text-right text-xs">{formatKg(ln.peso_caja_kg)}</TableCell>
                          <TableCell className="text-right text-xs font-medium">{formatKg(ln.peso_linea_kg)}</TableCell>
                          <TableCell className="text-right text-xs font-medium">
                            {formatPct(ln.peso_pct_total)}
                          </TableCell>
                          <TableCell className="text-xs">{FUENTE_LABEL[ln.fuente_peso] ?? ln.fuente_peso}</TableCell>
                          <TableCell><LineStatusIcon estado={ln.estado_linea} /></TableCell>
                          <TableCell className="text-right">
                            <div className="flex justify-end gap-1">
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 text-xs"
                                onClick={() => openEdit(ln)}
                              >
                                {ln.has_logistics_record ? "Editar logística" : "Crear logística"}
                              </Button>
                              <Button variant="ghost" size="icon" className="size-7" onClick={() => setDiagLine(ln)} title="Diagnóstico">
                                <Stethoscope className="size-3.5" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              {history.length > 0 ? (
                <Card>
                  <CardHeader><CardTitle className="text-base">Historial</CardTitle></CardHeader>
                  <CardContent className="overflow-x-auto p-0">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Fecha</TableHead>
                          <TableHead>Usuario</TableHead>
                          <TableHead className="text-right">Anterior</TableHead>
                          <TableHead className="text-right">Nuevo</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {history.map((h, i) => (
                          <TableRow key={i}>
                            <TableCell className="text-xs">{formatDt(h.created_at)}</TableCell>
                            <TableCell className="text-xs">{h.user_email || "—"}</TableCell>
                            <TableCell className="text-right text-xs">{formatKg(h.peso_anterior_kg)}</TableCell>
                            <TableCell className="text-right text-xs">{formatKg(h.peso_nuevo_kg)}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </CardContent>
                </Card>
              ) : null}
            </>
          ) : !orderLoading ? (
            <p className="py-16 text-center text-sm text-muted-foreground">Seleccione una orden del listado.</p>
          ) : null}
        </div>
      </div>

      <Dialog open={detectOpen} onOpenChange={setDetectOpen}>
        <DialogContent className="max-h-[85vh] max-w-2xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Productos sin peso — OC {order?.oc}</DialogTitle>
          </DialogHeader>
          {sinPesoLines.length === 0 ? (
            <p className="text-sm text-muted-foreground">No hay líneas sin peso en esta orden.</p>
          ) : (
            <div className="space-y-2">
              {sinPesoLines.map((ln) => (
                <div key={ln.detail_id} className="flex items-center justify-between gap-3 rounded-lg border p-3">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium">{ln.producto || ln.codigo}</p>
                    {ln.variante ? <p className="truncate text-xs text-muted-foreground">{ln.variante}</p> : null}
                    <p className="text-xs text-muted-foreground">Cant. {ln.cantidad_unitaria}</p>
                  </div>
                  <Button size="sm" onClick={() => { setDetectOpen(false); openEdit(ln) }}>
                    {ln.has_logistics_record ? "Editar logística" : "Crear logística"}
                  </Button>
                </div>
              ))}
            </div>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={!!editLine} onOpenChange={(o) => !o && setEditLine(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editLine?.has_logistics_record ? "Editar logística" : "Crear logística"}
            </DialogTitle>
          </DialogHeader>
          {editLine ? (
            <div className="grid gap-3">
              <div>
                <p className="text-sm font-medium">{editLine.producto}</p>
                {editLine.variante ? (
                  <p className="text-xs text-muted-foreground">{editLine.variante}</p>
                ) : null}
                <p className="font-mono text-xs text-muted-foreground">{editLine.codigo}</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Label>Unidades por caja</Label>
                  <Input
                    type="number"
                    value={editForm.units_per_box ?? ""}
                    onChange={(e) =>
                      setEditForm((f) => ({
                        ...f,
                        units_per_box: e.target.value ? Number(e.target.value) : undefined,
                      }))
                    }
                  />
                </div>
                <div>
                  <Label>Peso caja (kg)</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={editForm.weight_box_kg ?? ""}
                    onChange={(e) =>
                      setEditForm((f) => ({
                        ...f,
                        weight_box_kg: e.target.value ? Number(e.target.value) : undefined,
                      }))
                    }
                  />
                </div>
                <div>
                  <Label>Alto (cm)</Label>
                  <Input
                    type="number"
                    value={editForm.height_cm ?? ""}
                    onChange={(e) =>
                      setEditForm((f) => ({
                        ...f,
                        height_cm: e.target.value ? Number(e.target.value) : undefined,
                      }))
                    }
                  />
                </div>
                <div>
                  <Label>Ancho (cm)</Label>
                  <Input
                    type="number"
                    value={editForm.width_cm ?? ""}
                    onChange={(e) =>
                      setEditForm((f) => ({
                        ...f,
                        width_cm: e.target.value ? Number(e.target.value) : undefined,
                      }))
                    }
                  />
                </div>
                <div>
                  <Label>Largo (cm)</Label>
                  <Input
                    type="number"
                    value={editForm.length_cm ?? ""}
                    onChange={(e) =>
                      setEditForm((f) => ({
                        ...f,
                        length_cm: e.target.value ? Number(e.target.value) : undefined,
                      }))
                    }
                  />
                </div>
              </div>
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditLine(null)}>Cancelar</Button>
            <Button onClick={() => void saveEdit()} disabled={saving}>
              {saving ? <Loader2 className="mr-2 size-4 animate-spin" /> : <CheckCircle2 className="mr-2 size-4" />}
              Guardar
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!diagLine} onOpenChange={(o) => !o && setDiagLine(null)}>
        <DialogContent>
          <DialogHeader><DialogTitle>Diagnóstico técnico</DialogTitle></DialogHeader>
          {diagLine?.join_debug ? (
            <dl className="grid gap-2 text-sm">
              {Object.entries(diagLine.join_debug).map(([k, v]) => (
                <div key={k} className="flex justify-between gap-4 border-b py-1">
                  <dt className="text-muted-foreground">{k}</dt>
                  <dd className="font-mono text-xs">{String(v)}</dd>
                </div>
              ))}
            </dl>
          ) : (
            <p className="text-sm text-muted-foreground">Sin datos de diagnóstico.</p>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
