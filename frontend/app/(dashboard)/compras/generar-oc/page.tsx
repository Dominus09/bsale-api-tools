"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { FilePlus, Loader2, Printer } from "lucide-react"

import { OcInvoicePrint, triggerPrintInvoice } from "@/components/compras/oc-invoice"
import { Button } from "@/components/ui/button"
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
import {
  type Company,
  type PurchaseAnalysisRow,
  type PurchaseOrderDetailRow,
  type PurchaseOrderHeader,
  generatePurchaseOrderFromLines,
  getCompanies,
  getPurchaseAnalysis,
  getPurchaseOffices,
  getPurchaseOrder,
  getSuppliers,
  type Supplier,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const NONE = "__none__"

function rowKey(r: PurchaseAnalysisRow): string {
  return `a-${r.company_id}-${r.office_id}-${r.variant_id}`
}

function fmtNum(n: number | null | undefined, d = 0): string {
  if (n == null || !Number.isFinite(Number(n))) return "—"
  return Number(n).toLocaleString("es-CL", {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  })
}

function statusRowClass(status: string): string {
  switch (status) {
    case "COMPRAR":
      return "bg-emerald-500/12 hover:bg-emerald-500/18"
    case "REVISAR":
      return "bg-amber-400/15 hover:bg-amber-400/22"
    case "NO_COMPRAR":
      return "bg-red-500/12 hover:bg-red-500/18"
    default:
      return "bg-muted/40"
  }
}

type ManualDraft = {
  key: string
  product_type_name: string
  product_name: string
  variant_name: string
  barcode: string
  costo_bruto: number
  units_per_box: number | null
  cantidad: number
}

export default function GenerarOcPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<{ office_id: number; label: string }[]>([])
  const [suppliers, setSuppliers] = useState<Supplier[]>([])

  const [companyId, setCompanyId] = useState<string>(NONE)
  const [officeId, setOfficeId] = useState<string>(NONE)
  const [supplierId, setSupplierId] = useState<string>(NONE)

  const [analysis, setAnalysis] = useState<PurchaseAnalysisRow[]>([])
  const [loadingAnalysis, setLoadingAnalysis] = useState(false)
  const [error, setError] = useState("")

  const [qtyByKey, setQtyByKey] = useState<Record<string, number>>({})
  const [excluded, setExcluded] = useState<Set<string>>(() => new Set())
  const [manualRows, setManualRows] = useState<ManualDraft[]>([])

  const [addOpen, setAddOpen] = useState(false)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [resultOpen, setResultOpen] = useState(false)
  const [saving, setSaving] = useState(false)

  const [manualForm, setManualForm] = useState({
    product_type_name: "",
    product_name: "",
    variant_name: "",
    barcode: "",
    costo_bruto: "",
    units_per_box: "",
    cantidad: "",
  })

  const [confirmForm, setConfirmForm] = useState({
    fecha_entrega: "",
    forma_pago: "",
    responsable: "",
    observacion: "",
  })

  const [resultHeader, setResultHeader] = useState<PurchaseOrderHeader | null>(null)
  const [resultDetails, setResultDetails] = useState<PurchaseOrderDetailRow[]>([])
  const printHostId = "oc-invoice-print-host"

  const cid = companyId !== NONE ? parseInt(companyId, 10) : NaN
  const oid = officeId !== NONE ? parseInt(officeId, 10) : NaN
  const sid = supplierId !== NONE ? parseInt(supplierId, 10) : NaN
  const filtersReady = Number.isFinite(cid) && Number.isFinite(oid) && Number.isFinite(sid)

  useEffect(() => {
    getCompanies()
      .then(setCompanies)
      .catch(() => setCompanies([]))
    getSuppliers()
      .then((list) => setSuppliers(list.filter((s) => s.is_active !== false)))
      .catch(() => setSuppliers([]))
  }, [])

  useEffect(() => {
    if (!Number.isFinite(cid)) {
      setOffices([])
      setOfficeId(NONE)
      return
    }
    getPurchaseOffices(cid)
      .then(setOffices)
      .catch(() => setOffices([]))
    setOfficeId(NONE)
    setAnalysis([])
    setQtyByKey({})
    setExcluded(new Set())
    setManualRows([])
  }, [cid])

  const loadAnalysis = useCallback(async () => {
    if (!filtersReady) return
    setLoadingAnalysis(true)
    setError("")
    try {
      const rows = await getPurchaseAnalysis({
        companyId: cid,
        officeId: oid,
        supplierId: sid,
      })
      setAnalysis(rows)
      const q: Record<string, number> = {}
      for (const r of rows) {
        q[rowKey(r)] = Number(r.unidades_a_comprar) || 0
      }
      setQtyByKey(q)
      setExcluded(new Set())
    } catch (e) {
      setAnalysis([])
      setError(e instanceof Error ? e.message : "Error al cargar datos")
    } finally {
      setLoadingAnalysis(false)
    }
  }, [filtersReady, cid, oid, sid])

  useEffect(() => {
    if (filtersReady) void loadAnalysis()
    else {
      setAnalysis([])
      setQtyByKey({})
      setExcluded(new Set())
      setManualRows([])
    }
  }, [filtersReady, loadAnalysis])

  const updateQty = (key: string, raw: string) => {
    const n = parseFloat(raw.replace(",", "."))
    setQtyByKey((prev) => ({
      ...prev,
      [key]: Number.isFinite(n) && n >= 0 ? n : 0,
    }))
  }

  const toggleExclude = (key: string) => {
    setExcluded((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const openAddManual = () => {
    setManualForm({
      product_type_name: "",
      product_name: "",
      variant_name: "",
      barcode: "",
      costo_bruto: "",
      units_per_box: "",
      cantidad: "",
    })
    setAddOpen(true)
  }

  const saveManual = () => {
    const cost = parseFloat(manualForm.costo_bruto.replace(",", "."))
    const qty = parseFloat(manualForm.cantidad.replace(",", "."))
    const upb = manualForm.units_per_box.trim()
      ? parseInt(manualForm.units_per_box, 10)
      : null
    if (!manualForm.product_name.trim()) {
      setError("Indica nombre de producto")
      return
    }
    if (!Number.isFinite(qty) || qty <= 0) {
      setError("Cantidad inválida")
      return
    }
    if (!Number.isFinite(cost) || cost < 0) {
      setError("Costo inválido")
      return
    }
    const row: ManualDraft = {
      key: `m-${typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : String(Date.now())}`,
      product_type_name: manualForm.product_type_name.trim() || "",
      product_name: manualForm.product_name.trim(),
      variant_name: manualForm.variant_name.trim() || "",
      barcode: manualForm.barcode.trim() || "",
      costo_bruto: cost,
      units_per_box: upb != null && Number.isFinite(upb) && upb > 0 ? upb : null,
      cantidad: qty,
    }
    setManualRows((prev) => [...prev, row])
    setError("")
    setAddOpen(false)
  }

  const removeManual = (key: string) => {
    setManualRows((prev) => prev.filter((r) => r.key !== key))
  }

  const linesToSubmit = useMemo(() => {
    if (!filtersReady) return []
    const out: {
      variant_id: number | null
      product_type_name: string | null
      product_name: string | null
      variant_name: string | null
      barcode: string | null
      cantidad: number
      units_per_box: number | null
      costo_unitario: number
    }[] = []
    for (const r of analysis) {
      const k = rowKey(r)
      if (excluded.has(k)) continue
      const qty = qtyByKey[k] ?? 0
      if (qty <= 0) continue
      const upe = r.units_per_box_eff ?? 1
      out.push({
        variant_id: r.variant_id,
        product_type_name: r.product_type_name,
        product_name: r.product_name,
        variant_name: r.variant_name,
        barcode: r.barcode,
        cantidad: qty,
        units_per_box: r.units_per_box ?? upe,
        costo_unitario: Number(r.costo_bruto) || 0,
      })
    }
    for (const m of manualRows) {
      out.push({
        variant_id: null,
        product_type_name: m.product_type_name || null,
        product_name: m.product_name,
        variant_name: m.variant_name || null,
        barcode: m.barcode || null,
        cantidad: m.cantidad,
        units_per_box: m.units_per_box,
        costo_unitario: m.costo_bruto,
      })
    }
    return out
  }, [analysis, excluded, qtyByKey, manualRows, filtersReady])

  const openConfirm = () => {
    if (!filtersReady) {
      setError("Selecciona empresa, sucursal y proveedor")
      return
    }
    if (linesToSubmit.length === 0) {
      setError("No hay líneas para la orden (revisa cantidades o inclusiones)")
      return
    }
    setError("")
    setConfirmForm((f) => ({ ...f }))
    setConfirmOpen(true)
  }

  const submitOc = async () => {
    if (!filtersReady) return
    setSaving(true)
    setError("")
    try {
      const { oc_id } = await generatePurchaseOrderFromLines({
        company_id: cid,
        office_id: oid,
        supplier_id: sid,
        fecha_entrega: confirmForm.fecha_entrega.trim() || null,
        forma_pago: confirmForm.forma_pago.trim() || null,
        responsable: confirmForm.responsable.trim() || null,
        observacion: confirmForm.observacion.trim() || null,
        lines: linesToSubmit,
      })
      const full = await getPurchaseOrder(oc_id, { companyId: cid })
      setResultHeader(full.header)
      setResultDetails(full.details)
      setConfirmOpen(false)
      setResultOpen(true)
      void loadAnalysis()
      setManualRows([])
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo generar la OC")
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="mx-auto max-w-[1200px] space-y-8 pb-16">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">Generar orden de compra</h1>
        <p className="text-sm text-slate-500">
          Arma la compra con sugerencias por sucursal y proveedor; ajusta cantidades y confirma.
        </p>
      </div>

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{error}</div>
      ) : null}

      <section className="rounded-xl border border-slate-200/80 bg-white p-5 shadow-sm">
        <h2 className="mb-4 text-xs font-semibold uppercase tracking-wide text-slate-500">Contexto</h2>
        <div className="grid gap-4 sm:grid-cols-3">
          <div className="space-y-2">
            <Label>Empresa</Label>
            <Select value={companyId} onValueChange={setCompanyId}>
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Elegir…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NONE}>Elegir empresa</SelectItem>
                {companies.map((c) => (
                  <SelectItem key={c.company_id} value={String(c.company_id)}>
                    {c.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Sucursal</Label>
            <Select value={officeId} onValueChange={setOfficeId} disabled={!Number.isFinite(cid)}>
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Elegir…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NONE}>Elegir sucursal</SelectItem>
                {offices.map((o) => (
                  <SelectItem key={o.office_id} value={String(o.office_id)}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Proveedor</Label>
            <Select value={supplierId} onValueChange={setSupplierId}>
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Elegir…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NONE}>Elegir proveedor</SelectItem>
                {suppliers.map((s) => (
                  <SelectItem key={s.id} value={String(s.id)}>
                    {s.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="mt-6 flex flex-wrap gap-3 border-t border-slate-100 pt-5">
          <Button type="button" variant="outline" onClick={openAddManual} disabled={!filtersReady}>
            <FilePlus className="mr-2 size-4" />
            Agregar producto
          </Button>
          <Button type="button" onClick={openConfirm} disabled={!filtersReady || loadingAnalysis}>
            Generar orden de compra
          </Button>
          {loadingAnalysis ? (
            <span className="inline-flex items-center gap-2 text-sm text-slate-500">
              <Loader2 className="size-4 animate-spin" />
              Cargando sugerencias…
            </span>
          ) : null}
        </div>
      </section>

      <section className="rounded-xl border border-slate-200/80 bg-white shadow-sm">
        <div className="border-b border-slate-100 px-5 py-3">
          <h2 className="text-sm font-medium text-slate-800">Líneas sugeridas y manuales</h2>
          <p className="text-xs text-slate-500">
            Verde · comprar · Amarillo · revisar · Rojo · no comprar. Puedes excluir filas o editar cantidades.
          </p>
        </div>
        <div className="overflow-x-auto p-2">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-200 hover:bg-transparent">
                <TableHead className="w-10 text-center">Ok</TableHead>
                <TableHead>Estado</TableHead>
                <TableHead>Tipo</TableHead>
                <TableHead>Producto</TableHead>
                <TableHead>Variante</TableHead>
                <TableHead className="text-right">Stock</TableHead>
                <TableHead className="text-right">Venta 7d / 30d</TableHead>
                <TableHead className="text-right">Unid.</TableHead>
                <TableHead className="text-right">Cajas</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {manualRows.map((m) => {
                const upe = m.units_per_box && m.units_per_box > 0 ? m.units_per_box : 1
                const cajas = m.cantidad / upe
                const total = m.cantidad * m.costo_bruto
                return (
                  <TableRow key={m.key} className="border-slate-200 bg-sky-500/10">
                    <TableCell className="text-center">—</TableCell>
                    <TableCell>
                      <span className="rounded-full bg-sky-600/20 px-2 py-0.5 text-xs font-medium text-sky-900">
                        Manual
                      </span>
                    </TableCell>
                    <TableCell className="max-w-[120px] truncate">{m.product_type_name || "—"}</TableCell>
                    <TableCell className="max-w-[160px] font-medium">{m.product_name}</TableCell>
                    <TableCell className="max-w-[160px] truncate">{m.variant_name || "—"}</TableCell>
                    <TableCell className="text-right">—</TableCell>
                    <TableCell className="text-right">—</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtNum(m.cantidad, 2)}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtNum(cajas, 2)}</TableCell>
                    <TableCell className="text-right tabular-nums font-medium">{fmtNum(total, 0)}</TableCell>
                    <TableCell>
                      <Button type="button" variant="ghost" size="sm" className="text-red-600" onClick={() => removeManual(m.key)}>
                        Quitar
                      </Button>
                    </TableCell>
                  </TableRow>
                )
              })}
              {analysis.length === 0 && manualRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={11} className="py-12 text-center text-slate-500">
                    {filtersReady
                      ? "No hay filas para este contexto."
                      : "Elige empresa, sucursal y proveedor para ver sugerencias."}
                  </TableCell>
                </TableRow>
              ) : null}
              {analysis.map((r) => {
                const k = rowKey(r)
                const ex = excluded.has(k)
                const qty = qtyByKey[k] ?? 0
                const upe = r.units_per_box_eff && r.units_per_box_eff > 0 ? r.units_per_box_eff : 1
                const cajas = qty / upe
                const unit = Number(r.costo_bruto) || 0
                const total = qty * unit
                return (
                  <TableRow key={k} className={cn("border-slate-200", statusRowClass(r.status), ex && "opacity-40")}>
                    <TableCell className="text-center">
                      <input
                        type="checkbox"
                        checked={!ex}
                        onChange={() => toggleExclude(k)}
                        className="size-4 accent-slate-900"
                        aria-label="Incluir en la orden"
                      />
                    </TableCell>
                    <TableCell>
                      <span
                        className={cn(
                          "rounded-full px-2 py-0.5 text-xs font-medium",
                          r.status === "COMPRAR" && "bg-emerald-600/25 text-emerald-900",
                          r.status === "REVISAR" && "bg-amber-500/30 text-amber-950",
                          r.status === "NO_COMPRAR" && "bg-red-600/25 text-red-900",
                        )}
                      >
                        {r.status}
                      </span>
                    </TableCell>
                    <TableCell className="max-w-[120px] truncate text-slate-600">{r.product_type_name ?? "—"}</TableCell>
                    <TableCell className="max-w-[160px] font-medium text-slate-900">{r.product_name ?? "—"}</TableCell>
                    <TableCell className="max-w-[180px] truncate text-slate-600">{r.variant_name ?? "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{fmtNum(r.stock_actual, 0)}</TableCell>
                    <TableCell className="text-right tabular-nums text-xs text-slate-600">
                      {fmtNum(r.ventas_7_dias, 0)} / {fmtNum(r.ventas_30_dias, 0)}
                    </TableCell>
                    <TableCell className="text-right">
                      <Input
                        className="h-8 w-20 tabular-nums text-right"
                        value={String(qty)}
                        disabled={ex}
                        onChange={(e) => updateQty(k, e.target.value)}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-slate-700">{fmtNum(cajas, 2)}</TableCell>
                    <TableCell className="text-right tabular-nums font-medium text-slate-900">{fmtNum(total, 0)}</TableCell>
                    <TableCell />
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      </section>

      <Dialog open={addOpen} onOpenChange={setAddOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Agregar producto manual</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3 py-2">
            <div className="space-y-1">
              <Label>Tipo producto</Label>
              <Input
                value={manualForm.product_type_name}
                onChange={(e) => setManualForm((f) => ({ ...f, product_type_name: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Producto</Label>
              <Input
                value={manualForm.product_name}
                onChange={(e) => setManualForm((f) => ({ ...f, product_name: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Variante</Label>
              <Input
                value={manualForm.variant_name}
                onChange={(e) => setManualForm((f) => ({ ...f, variant_name: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Código de barras</Label>
              <Input
                value={manualForm.barcode}
                onChange={(e) => setManualForm((f) => ({ ...f, barcode: e.target.value }))}
              />
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="space-y-1">
                <Label>Costo unitario</Label>
                <Input
                  inputMode="decimal"
                  value={manualForm.costo_bruto}
                  onChange={(e) => setManualForm((f) => ({ ...f, costo_bruto: e.target.value }))}
                />
              </div>
              <div className="space-y-1">
                <Label>Unid. por caja</Label>
                <Input
                  inputMode="numeric"
                  value={manualForm.units_per_box}
                  onChange={(e) => setManualForm((f) => ({ ...f, units_per_box: e.target.value }))}
                />
              </div>
            </div>
            <div className="space-y-1">
              <Label>Cantidad</Label>
              <Input
                inputMode="decimal"
                value={manualForm.cantidad}
                onChange={(e) => setManualForm((f) => ({ ...f, cantidad: e.target.value }))}
              />
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setAddOpen(false)}>
              Cancelar
            </Button>
            <Button type="button" onClick={saveManual}>
              Agregar
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Confirmar orden de compra</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3 py-2 text-sm">
            <p className="text-slate-600">
              Proveedor:{" "}
              <span className="font-medium text-slate-900">
                {suppliers.find((s) => s.id === sid)?.name ?? `#${sid}`}
              </span>
            </p>
            <div className="space-y-1">
              <Label>Fecha entrega</Label>
              <Input
                type="date"
                value={confirmForm.fecha_entrega}
                onChange={(e) => setConfirmForm((f) => ({ ...f, fecha_entrega: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Forma de pago</Label>
              <Input
                value={confirmForm.forma_pago}
                onChange={(e) => setConfirmForm((f) => ({ ...f, forma_pago: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Responsable</Label>
              <Input
                value={confirmForm.responsable}
                onChange={(e) => setConfirmForm((f) => ({ ...f, responsable: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label>Observación</Label>
              <Input
                value={confirmForm.observacion}
                onChange={(e) => setConfirmForm((f) => ({ ...f, observacion: e.target.value }))}
              />
            </div>
            <p className="text-xs text-slate-500">{linesToSubmit.length} líneas en esta orden.</p>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setConfirmOpen(false)} disabled={saving}>
              Volver
            </Button>
            <Button type="button" onClick={() => void submitOc()} disabled={saving}>
              {saving ? <Loader2 className="size-4 animate-spin" /> : "Confirmar"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={resultOpen} onOpenChange={setResultOpen}>
        <DialogContent className="max-h-[90vh] max-w-3xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Orden generada</DialogTitle>
          </DialogHeader>
          {resultHeader ? (
            <div id={printHostId} className="py-2">
              <OcInvoicePrint header={resultHeader} details={resultDetails} />
            </div>
          ) : null}
          <DialogFooter className="gap-2 sm:justify-end">
            <Button type="button" variant="outline" onClick={() => setResultOpen(false)}>
              Cerrar
            </Button>
            <Button type="button" variant="secondary" onClick={() => triggerPrintInvoice(printHostId)}>
              <Printer className="mr-2 size-4" />
              Imprimir / PDF
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
