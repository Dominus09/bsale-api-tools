"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  CheckCircle2,
  Download,
  Loader2,
  PackageCheck,
  Pencil,
  RefreshCw,
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

function SemaforoBadge({ semaforo, pct }: { semaforo: string; pct: number }) {
  const cls =
    semaforo === "verde" || semaforo === "verde_claro"
      ? "bg-emerald-600"
      : semaforo === "amarillo"
        ? "bg-amber-500"
        : semaforo === "naranja"
          ? "bg-orange-500"
          : "bg-red-600"
  return (
    <Badge className={cn("text-white", cls)}>
      {formatPct(pct)} cobertura
    </Badge>
  )
}

export default function PesoOrdenesPage() {
  const [oc, setOc] = useState("")
  const [cliente, setCliente] = useState("")
  const [codigoCliente, setCodigoCliente] = useState("")
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [estado, setEstado] = useState(ALL)
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
        limit: 150,
      })
      setResults(rows)
    } catch (e) {
      setSearchError(e instanceof Error ? e.message : "Error en búsqueda")
    } finally {
      setSearching(false)
    }
  }, [oc, cliente, codigoCliente, dateFrom, dateTo, estado])

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
      let pmId = editLine.products_master_id
      if (!pmId && editLine.variant_id) {
        const created = await createOrderWeightLogistics(editLine.variant_id, order.document_id)
        setOrder(created.order)
        setEditLine(null)
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
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Logística
        </p>
        <h1 className="flex items-center gap-2 text-2xl font-semibold">
          <PackageCheck className="size-7 text-primary" />
          Peso de Órdenes
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Fuente oficial de peso para planificación, camiones y ORS.
        </p>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Buscador</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3 lg:grid-cols-6">
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
            <Label>Desde</Label>
            <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </div>
          <div>
            <Label>Hasta</Label>
            <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </div>
          <div>
            <Label>Estado</Label>
            <Select value={estado} onValueChange={setEstado}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>Todos</SelectItem>
                <SelectItem value="completo">Completo</SelectItem>
                <SelectItem value="parcial">Parcial</SelectItem>
                <SelectItem value="sin_peso">Sin peso</SelectItem>
                <SelectItem value="pendiente">Pendiente</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-end md:col-span-3 lg:col-span-6">
            <Button onClick={() => void runSearch()} disabled={searching}>
              {searching ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Search className="mr-2 size-4" />}
              Buscar
            </Button>
          </div>
        </CardContent>
      </Card>

      {searchError ? (
        <Alert variant="destructive"><AlertDescription>{searchError}</AlertDescription></Alert>
      ) : null}

      <div className="grid gap-6 lg:grid-cols-5">
        <Card className="lg:col-span-2">
          <CardHeader><CardTitle className="text-base">Órdenes</CardTitle></CardHeader>
          <CardContent className="max-h-[520px] overflow-y-auto p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>OC</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead className="text-right">Peso</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {results.map((r) => (
                  <TableRow
                    key={r.document_id}
                    className={cn("cursor-pointer", selectedId === r.document_id && "bg-muted/60")}
                    onClick={() => void loadOrder(r.document_id)}
                  >
                    <TableCell className="font-mono">{r.oc}</TableCell>
                    <TableCell className="max-w-[140px] truncate">{r.cliente || "—"}</TableCell>
                    <TableCell className="text-right text-xs tabular-nums">
                      {formatKg(r.peso_total_kg ?? undefined)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <div className="flex flex-col gap-4 lg:col-span-3">
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
                <CardHeader className="flex flex-row flex-wrap items-start justify-between gap-3">
                  <div>
                    <CardTitle>OC {order.oc}</CardTitle>
                    <p className="text-sm text-muted-foreground">{order.cliente}</p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button variant="outline" size="sm" onClick={() => void handleRecalculate()} disabled={recalculating}>
                      {recalculating ? <Loader2 className="mr-1 size-4 animate-spin" /> : <RefreshCw className="mr-1 size-4" />}
                      Recalcular Orden
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => void handleExport()}>
                      <Download className="mr-1 size-4" /> Exportar
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-3">
                  <div><span className="text-muted-foreground">Empresa:</span> {order.empresa || "—"}</div>
                  <div><span className="text-muted-foreground">Monto:</span> {formatMoney(order.total_amount)}</div>
                  <div><span className="text-muted-foreground">Productos:</span> {order.productos_totales}</div>
                  <div><span className="text-muted-foreground">Peso total:</span> <strong>{formatKg(order.peso_total_kg)}</strong></div>
                  <div className="flex items-center gap-2">
                    <span className="text-muted-foreground">Cobertura:</span>
                    <SemaforoBadge semaforo={order.semaforo} pct={order.porcentaje_cobertura} />
                  </div>
                  <div><span className="text-muted-foreground">Sin peso:</span> {order.productos_sin_peso}</div>
                  <div><span className="text-muted-foreground">Manuales:</span> {order.productos_manuales}</div>
                  <div><span className="text-muted-foreground">Estimados:</span> {order.productos_estimados}</div>
                  <div><span className="text-muted-foreground">Último cálculo:</span> {formatDt(order.ultimo_calculo)}</div>
                </CardContent>
              </Card>

              {history.length > 0 ? (
                <Card>
                  <CardHeader><CardTitle className="text-base">Historial de cálculos</CardTitle></CardHeader>
                  <CardContent className="overflow-x-auto p-0">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Fecha</TableHead>
                          <TableHead>Usuario</TableHead>
                          <TableHead className="text-right">Peso anterior</TableHead>
                          <TableHead className="text-right">Peso nuevo</TableHead>
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
                        <TableHead className="text-right">Cant. unit.</TableHead>
                        <TableHead className="text-right">Cant. cajas</TableHead>
                        <TableHead className="text-right">Peso unit.</TableHead>
                        <TableHead className="text-right">Peso caja</TableHead>
                        <TableHead className="text-right">Peso línea</TableHead>
                        <TableHead>Fuente</TableHead>
                        <TableHead>Est.</TableHead>
                        <TableHead />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredLines.map((ln) => (
                        <TableRow key={ln.detail_id} className={ln.estado_linea === "sin_peso" ? "bg-destructive/5" : undefined}>
                          <TableCell className="font-mono text-xs">{ln.codigo || "—"}</TableCell>
                          <TableCell className="max-w-[180px]">
                            <div className="truncate text-sm">{ln.producto || "—"}</div>
                            {ln.variante ? <div className="truncate text-xs text-muted-foreground">{ln.variante}</div> : null}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{ln.cantidad_unitaria}</TableCell>
                          <TableCell className="text-right tabular-nums">
                            {ln.cantidad_cajas != null ? ln.cantidad_cajas : "—"}
                          </TableCell>
                          <TableCell className="text-right text-xs">{formatKg(ln.peso_unitario_kg)}</TableCell>
                          <TableCell className="text-right text-xs">{formatKg(ln.peso_caja_kg)}</TableCell>
                          <TableCell className="text-right text-xs font-medium">{formatKg(ln.peso_linea_kg)}</TableCell>
                          <TableCell className="text-xs">{FUENTE_LABEL[ln.fuente_peso] ?? ln.fuente_peso}</TableCell>
                          <TableCell><LineStatusIcon estado={ln.estado_linea} /></TableCell>
                          <TableCell className="space-x-1 text-right">
                            {!ln.has_logistics_record && ln.variant_id ? (
                              <Button
                                variant="outline"
                                size="sm"
                                className="text-xs"
                                onClick={() => openEdit(ln)}
                              >
                                Crear ficha
                              </Button>
                            ) : (
                              <Button variant="ghost" size="icon" onClick={() => openEdit(ln)} title="Editar">
                                <Pencil className="size-4" />
                              </Button>
                            )}
                            <Button variant="ghost" size="icon" onClick={() => setDiagLine(ln)} title="Diagnóstico">
                              <Stethoscope className="size-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </>
          ) : !orderLoading ? (
            <p className="py-16 text-center text-sm text-muted-foreground">
              Seleccione una orden de la lista.
            </p>
          ) : null}
        </div>
      </div>

      <Dialog open={!!editLine} onOpenChange={(o) => !o && setEditLine(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editLine?.has_logistics_record ? "Editar logística" : "Crear ficha logística"}
            </DialogTitle>
          </DialogHeader>
          {editLine ? (
            <div className="grid gap-3">
              <p className="text-sm font-medium">{editLine.producto}</p>
              <p className="font-mono text-xs text-muted-foreground">{editLine.codigo}</p>
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
          <DialogHeader>
            <DialogTitle>Diagnóstico técnico</DialogTitle>
          </DialogHeader>
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
