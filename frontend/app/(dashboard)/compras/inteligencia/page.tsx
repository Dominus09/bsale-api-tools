"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, RefreshCw, Trash2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
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
import {
  createPurchaseManualItem,
  deletePurchaseManualItem,
  generatePurchaseOrder,
  getPurchaseAnalysis,
  getPurchaseManualItems,
  getPurchaseOrder,
  getPurchaseOrders,
  getSuppliers,
  type PurchaseAnalysisRow,
  type PurchaseManualItem,
  type PurchaseOrderDetailRow,
  type PurchaseOrderHeader,
  type Supplier,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const OFFICE_STORAGE_KEY = "purchase_office_id"
const STATUS_OPTIONS = [
  { value: "all", label: "Todos" },
  { value: "COMPRAR", label: "Comprar" },
  { value: "REVISAR", label: "Revisar" },
  { value: "NO_COMPRAR", label: "No comprar" },
] as const

function fmtNum(n: number | null | undefined, digits = 2): string {
  if (n == null || Number.isNaN(n)) return "—"
  return Number(n).toLocaleString("es-CL", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

function statusClass(status: string): string {
  switch (status) {
    case "COMPRAR":
      return "bg-emerald-600/15 text-emerald-800 dark:text-emerald-200"
    case "REVISAR":
      return "bg-amber-500/15 text-amber-900 dark:text-amber-100"
    case "NO_COMPRAR":
      return "bg-muted text-muted-foreground"
    default:
      return "bg-muted"
  }
}

export default function ComprasInteligenciaPage() {
  const [officeInput, setOfficeInput] = useState("")
  const [statusFilter, setStatusFilter] = useState<string>("all")
  const [rows, setRows] = useState<PurchaseAnalysisRow[]>([])
  const [orders, setOrders] = useState<PurchaseOrderHeader[]>([])
  const [manualItems, setManualItems] = useState<PurchaseManualItem[]>([])
  const [suppliers, setSuppliers] = useState<Supplier[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [genSupplierId, setGenSupplierId] = useState<string>("")
  const [genFechaEntrega, setGenFechaEntrega] = useState("")
  const [genFormaPago, setGenFormaPago] = useState("")
  const [genResponsable, setGenResponsable] = useState("")
  const [genObs, setGenObs] = useState("")
  const [generating, setGenerating] = useState(false)
  const [manualSupplierId, setManualSupplierId] = useState<string>("")
  const [manualProduct, setManualProduct] = useState("")
  const [manualCantidad, setManualCantidad] = useState("")
  const [manualCosto, setManualCosto] = useState("")
  const [manualSaving, setManualSaving] = useState(false)
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailHeader, setDetailHeader] = useState<PurchaseOrderHeader | null>(null)
  const [detailLines, setDetailLines] = useState<PurchaseOrderDetailRow[]>([])

  const officeId = useMemo(() => {
    const n = parseInt(officeInput.trim(), 10)
    return Number.isFinite(n) && n > 0 ? n : null
  }, [officeInput])

  useEffect(() => {
    if (typeof window === "undefined") return
    const stored = sessionStorage.getItem(OFFICE_STORAGE_KEY)
    if (stored && stored.trim()) {
      setOfficeInput(stored.trim())
    }
  }, [])

  useEffect(() => {
    if (typeof window === "undefined" || !officeInput.trim()) return
    sessionStorage.setItem(OFFICE_STORAGE_KEY, officeInput.trim())
  }, [officeInput])

  const loadSuppliers = useCallback(async () => {
    const list = await getSuppliers()
    setSuppliers(list.filter((s) => s.is_active !== false))
  }, [])

  const loadAnalysis = useCallback(async () => {
    if (officeId == null) {
      setError("Indica un office_id numérico (sucursal Bsale).")
      return
    }
    setLoading(true)
    setError(null)
    try {
      const data = await getPurchaseAnalysis(officeId, {
        status: statusFilter === "all" ? undefined : statusFilter,
      })
      setRows(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
      setRows([])
    } finally {
      setLoading(false)
    }
  }, [officeId, statusFilter])

  const loadOrdersAndManual = useCallback(async () => {
    if (officeId == null) return
    try {
      const [o, m] = await Promise.all([
        getPurchaseOrders(officeId),
        getPurchaseManualItems(officeId),
      ])
      setOrders(o)
      setManualItems(m)
    } catch {
      setOrders([])
      setManualItems([])
    }
  }, [officeId])

  useEffect(() => {
    loadSuppliers().catch(() => setSuppliers([]))
  }, [loadSuppliers])

  useEffect(() => {
    if (officeId != null) {
      loadAnalysis()
      loadOrdersAndManual()
    }
  }, [officeId, loadAnalysis, loadOrdersAndManual])

  const companyId = useMemo(() => {
    if (typeof window === "undefined") return null
    const n = parseInt(localStorage.getItem("company_id") || "", 10)
    return Number.isFinite(n) && n > 0 ? n : null
  }, [])

  async function onGenerateOc() {
    if (officeId == null || companyId == null) {
      setError("Empresa u office_id inválido.")
      return
    }
    const sid = parseInt(genSupplierId, 10)
    if (!Number.isFinite(sid) || sid <= 0) {
      setError("Selecciona un proveedor para la cabecera de la OC.")
      return
    }
    setGenerating(true)
    setError(null)
    try {
      const manualIds = manualItems
        .filter((x) => x.supplier_id === sid && x.oc_id == null)
        .map((x) => x.id)
      const payload = {
        company_id: companyId,
        office_id: officeId,
        supplier_id: sid,
        fecha_entrega: genFechaEntrega.trim() || null,
        forma_pago: genFormaPago.trim() || null,
        responsable: genResponsable.trim() || null,
        observacion: genObs.trim() || null,
        manual_ids: manualIds.length ? manualIds : null,
      }
      const { oc_id } = await generatePurchaseOrder(payload)
      await Promise.all([loadAnalysis(), loadOrdersAndManual()])
      setGenObs("")
      alert(`OC generada: #${oc_id}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al generar OC")
    } finally {
      setGenerating(false)
    }
  }

  async function onAddManual() {
    if (officeId == null || companyId == null) return
    const sid = parseInt(manualSupplierId, 10)
    const qty = parseFloat(manualCantidad.replace(",", "."))
    const cost = manualCosto.trim() ? parseFloat(manualCosto.replace(",", ".")) : undefined
    if (!Number.isFinite(sid) || sid <= 0) {
      setError("Selecciona proveedor para el ítem manual.")
      return
    }
    if (!Number.isFinite(qty) || qty <= 0) {
      setError("Cantidad debe ser un número mayor a 0.")
      return
    }
    setManualSaving(true)
    setError(null)
    try {
      await createPurchaseManualItem({
        company_id: companyId,
        office_id: officeId,
        supplier_id: sid,
        product_name: manualProduct.trim() || null,
        cantidad: qty,
        costo_bruto: cost != null && Number.isFinite(cost) ? cost : null,
      })
      setManualProduct("")
      setManualCantidad("")
      setManualCosto("")
      await loadOrdersAndManual()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al guardar ítem")
    } finally {
      setManualSaving(false)
    }
  }

  async function onDeleteManual(id: number) {
    if (!confirm("¿Eliminar este ítem manual pendiente?")) return
    try {
      await deletePurchaseManualItem(id)
      await loadOrdersAndManual()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al eliminar")
    }
  }

  async function openOrderDetail(ocId: number) {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetailHeader(null)
    setDetailLines([])
    try {
      const d = await getPurchaseOrder(ocId)
      setDetailHeader(d.header)
      setDetailLines(d.details)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar detalle")
      setDetailOpen(false)
    } finally {
      setDetailLoading(false)
    }
  }

  const comprarCount = rows.filter((r) => r.status === "COMPRAR").length

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Compras inteligentes</h1>
        <p className="text-sm text-muted-foreground">
          Vista <code className="rounded bg-muted px-1">bsale.vw_purchase_analysis</code>, generación de OC y líneas
          manuales. El proveedor de la cabecera aplica a metadatos y a ítems manuales del mismo proveedor; las líneas
          automáticas son todas las sugerencias <span className="font-medium">COMPRAR</span> de la sucursal.
        </p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/50 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      )}

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Sucursal y filtros</CardTitle>
          <CardDescription>
            <code className="text-xs">office_id</code> es el id de sucursal en Bsale (se guarda en esta sesión).
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap items-end gap-4">
          <div className="space-y-2">
            <Label htmlFor="office">Office ID</Label>
            <Input
              id="office"
              className="w-40"
              placeholder="ej. 1"
              value={officeInput}
              onChange={(e) => setOfficeInput(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label>Estado</Label>
            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-44">
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                {STATUS_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <Button type="button" variant="secondary" onClick={() => loadAnalysis()} disabled={loading || officeId == null}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            <span className="ml-2">Actualizar análisis</span>
          </Button>
        </CardContent>
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card className="lg:col-span-2">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Sugerencias de compra</CardTitle>
            <CardDescription>
              {officeId == null
                ? "Indica office_id para cargar datos."
                : `${rows.length} filas · ${comprarCount} con estado COMPRAR`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="max-h-[480px] overflow-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Estado</TableHead>
                    <TableHead>Producto</TableHead>
                    <TableHead>Variante</TableHead>
                    <TableHead className="text-right">Stock</TableHead>
                    <TableHead className="text-right">Unid. compra</TableHead>
                    <TableHead className="text-right">Cajas</TableHead>
                    <TableHead className="text-right">Total $</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.length === 0 && !loading ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center text-muted-foreground">
                        Sin datos
                      </TableCell>
                    </TableRow>
                  ) : (
                    rows.map((r) => (
                      <TableRow key={`${r.variant_id}-${r.office_id}`}>
                        <TableCell>
                          <span
                            className={cn(
                              "inline-flex rounded-full px-2 py-0.5 text-xs font-medium",
                              statusClass(r.status),
                            )}
                          >
                            {r.status}
                          </span>
                        </TableCell>
                        <TableCell className="max-w-[200px] truncate" title={r.product_name ?? ""}>
                          {r.product_name ?? "—"}
                        </TableCell>
                        <TableCell className="max-w-[220px] truncate" title={r.variant_name ?? ""}>
                          {r.variant_name ?? "—"}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{fmtNum(r.stock_actual, 0)}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmtNum(r.unidades_a_comprar, 2)}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmtNum(r.cajas_sugeridas, 2)}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmtNum(r.costo_total_compra, 0)}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Generar orden de compra</CardTitle>
            <CardDescription>
              Incluye líneas COMPRAR de la sucursal e ítems manuales pendientes del proveedor elegido.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-2">
              <Label>Proveedor (cabecera)</Label>
              <Select value={genSupplierId || undefined} onValueChange={setGenSupplierId}>
                <SelectTrigger>
                  <SelectValue placeholder="Seleccionar…" />
                </SelectTrigger>
                <SelectContent>
                  {suppliers.map((s) => (
                    <SelectItem key={s.id} value={String(s.id)}>
                      {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="fe">Fecha entrega (opcional)</Label>
              <Input id="fe" type="date" value={genFechaEntrega} onChange={(e) => setGenFechaEntrega(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="fp">Forma de pago</Label>
              <Input id="fp" value={genFormaPago} onChange={(e) => setGenFormaPago(e.target.value)} placeholder="Opcional" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="resp">Responsable</Label>
              <Input id="resp" value={genResponsable} onChange={(e) => setGenResponsable(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="obs">Observación</Label>
              <Input id="obs" value={genObs} onChange={(e) => setGenObs(e.target.value)} />
            </div>
            <Button
              type="button"
              className="w-full"
              disabled={generating || officeId == null}
              onClick={() => void onGenerateOc()}
            >
              {generating ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              <span className={generating ? "ml-2" : ""}>Generar OC</span>
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Ítem manual (pendiente)</CardTitle>
            <CardDescription>Se adjuntan a la próxima OC del mismo proveedor al generar.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-2">
              <Label>Proveedor</Label>
              <Select value={manualSupplierId || undefined} onValueChange={setManualSupplierId}>
                <SelectTrigger>
                  <SelectValue placeholder="Seleccionar…" />
                </SelectTrigger>
                <SelectContent>
                  {suppliers.map((s) => (
                    <SelectItem key={s.id} value={String(s.id)}>
                      {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="mp">Descripción producto</Label>
              <Input id="mp" value={manualProduct} onChange={(e) => setManualProduct(e.target.value)} />
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="space-y-2">
                <Label htmlFor="mq">Cantidad</Label>
                <Input id="mq" value={manualCantidad} onChange={(e) => setManualCantidad(e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="mc">Costo bruto</Label>
                <Input id="mc" value={manualCosto} onChange={(e) => setManualCosto(e.target.value)} />
              </div>
            </div>
            <Button type="button" variant="secondary" className="w-full" disabled={manualSaving || officeId == null} onClick={() => void onAddManual()}>
              {manualSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : "Agregar ítem"}
            </Button>
            <div className="max-h-48 overflow-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Producto</TableHead>
                    <TableHead className="text-right">Cant.</TableHead>
                    <TableHead className="w-10" />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {manualItems.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={3} className="text-center text-muted-foreground">
                        Sin pendientes
                      </TableCell>
                    </TableRow>
                  ) : (
                    manualItems.map((m) => (
                      <TableRow key={m.id}>
                        <TableCell className="max-w-[180px] truncate">{m.product_name ?? "—"}</TableCell>
                        <TableCell className="text-right tabular-nums">{fmtNum(m.cantidad, 2)}</TableCell>
                        <TableCell>
                          <Button type="button" variant="ghost" size="icon" className="h-8 w-8" onClick={() => void onDeleteManual(m.id)}>
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Órdenes recientes (sucursal)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>OC</TableHead>
                  <TableHead>Proveedor</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead className="text-right">Total</TableHead>
                  <TableHead>Emisión</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {orders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-muted-foreground">
                      {officeId == null ? "Indica office_id" : "Sin órdenes"}
                    </TableCell>
                  </TableRow>
                ) : (
                  orders.map((o) => (
                    <TableRow
                      key={o.oc_id}
                      className="cursor-pointer"
                      onClick={() => void openOrderDetail(o.oc_id)}
                    >
                      <TableCell className="font-mono text-xs">#{o.oc_id}</TableCell>
                      <TableCell>{o.supplier_name ?? o.supplier_id}</TableCell>
                      <TableCell>{o.status}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtNum(o.total_oc, 0)}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {o.fecha_emision ? String(o.fecha_emision).slice(0, 10) : "—"}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-w-3xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {detailHeader ? `OC #${detailHeader.oc_id}` : "Detalle OC"}
            </DialogTitle>
          </DialogHeader>
          {detailLoading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : detailHeader ? (
            <div className="space-y-4 text-sm">
              <div className="grid grid-cols-2 gap-2 text-muted-foreground">
                <div>
                  Proveedor: <span className="text-foreground">{detailHeader.supplier_name ?? detailHeader.supplier_id}</span>
                </div>
                <div>
                  Total:{" "}
                  <span className="text-foreground tabular-nums">{fmtNum(detailHeader.total_oc, 0)}</span>
                </div>
                <div className="col-span-2">{detailHeader.observacion || "—"}</div>
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Producto</TableHead>
                    <TableHead className="text-right">Cant.</TableHead>
                    <TableHead className="text-right">Unit.</TableHead>
                    <TableHead className="text-right">Total</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {detailLines.map((l) => (
                    <TableRow key={l.oc_detail_id}>
                      <TableCell className="max-w-[240px]">{l.product_name ?? l.variant_name ?? "—"}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtNum(l.cantidad, 2)}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtNum(l.costo_unitario, 0)}</TableCell>
                      <TableCell className="text-right tabular-nums">{fmtNum(l.costo_total, 0)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
