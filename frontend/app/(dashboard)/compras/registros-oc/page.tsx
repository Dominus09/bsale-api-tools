"use client"

import { useEffect, useState } from "react"
import { Eye, Loader2, Printer } from "lucide-react"

import { ComprasDataStatusCard } from "@/components/compras/compras-data-status"
import { OcInvoicePrint, triggerPrintInvoice } from "@/components/compras/oc-invoice"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
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
  type PurchaseOfficeRef,
  type PurchaseOrderDetailRow,
  type PurchaseOrderHeader,
  getCompanies,
  getPurchaseOffices,
  getPurchaseOrder,
  getPurchaseOrders,
  patchPurchaseOrderStatus,
} from "@/lib/api"

const NONE = "__none__"

const STATUS_OPTIONS = ["BORRADOR", "GENERADA", "ENVIADA", "RECIBIDA", "ANULADA"] as const

function fmtDate(s: string | null | undefined): string {
  if (!s) return "—"
  return String(s).slice(0, 10)
}

function fmtMoney(n: number | string | null | undefined): string {
  const x = typeof n === "string" ? parseFloat(n) : Number(n)
  if (!Number.isFinite(x)) return "—"
  return x.toLocaleString("es-CL", { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

export default function RegistrosOcPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<PurchaseOfficeRef[]>([])
  const [companyId, setCompanyId] = useState<string>(NONE)
  const [officeId, setOfficeId] = useState<string>(NONE)

  const [rows, setRows] = useState<PurchaseOrderHeader[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailHeader, setDetailHeader] = useState<PurchaseOrderHeader | null>(null)
  const [detailLines, setDetailLines] = useState<PurchaseOrderDetailRow[]>([])

  const [statusOpen, setStatusOpen] = useState(false)
  const [statusOcId, setStatusOcId] = useState<number | null>(null)
  const [statusValue, setStatusValue] = useState<string>("GENERADA")
  const [statusSaving, setStatusSaving] = useState(false)
  const [autoPrintAfterOpen, setAutoPrintAfterOpen] = useState(false)

  const printHostId = "reg-oc-invoice-print"

  const cid = companyId !== NONE ? parseInt(companyId, 10) : NaN
  const oid = officeId !== NONE ? parseInt(officeId, 10) : NaN

  useEffect(() => {
    getCompanies()
      .then(setCompanies)
      .catch(() => setCompanies([]))
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
  }, [cid])

  const loadOrders = async () => {
    if (!Number.isFinite(cid)) {
      setRows([])
      return
    }
    setLoading(true)
    setError("")
    try {
      const list = await getPurchaseOrders({
        companyId: cid,
        officeId: Number.isFinite(oid) ? oid : undefined,
      })
      setRows(list)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
      setRows([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadOrders()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cid, oid])

  useEffect(() => {
    if (!detailOpen || !autoPrintAfterOpen || !detailHeader || detailLoading) return
    const t = window.setTimeout(() => {
      triggerPrintInvoice(printHostId)
      setAutoPrintAfterOpen(false)
    }, 450)
    return () => window.clearTimeout(t)
  }, [detailOpen, autoPrintAfterOpen, detailHeader, detailLoading])

  const openDetail = async (ocId: number) => {
    if (!Number.isFinite(cid)) return
    setAutoPrintAfterOpen(false)
    setDetailOpen(true)
    setDetailLoading(true)
    setDetailHeader(null)
    setDetailLines([])
    try {
      const d = await getPurchaseOrder(ocId, { companyId: cid })
      setDetailHeader(d.header)
      setDetailLines(d.details)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar detalle")
      setDetailOpen(false)
    } finally {
      setDetailLoading(false)
    }
  }

  const openStatus = (oc: PurchaseOrderHeader) => {
    setStatusOcId(oc.oc_id)
    setStatusValue(oc.status || "GENERADA")
    setStatusOpen(true)
  }

  const saveStatus = async () => {
    if (statusOcId == null || !Number.isFinite(cid)) return
    setStatusSaving(true)
    try {
      await patchPurchaseOrderStatus(statusOcId, statusValue, cid)
      setStatusOpen(false)
      await loadOrders()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al guardar estado")
    } finally {
      setStatusSaving(false)
    }
  }

  return (
    <div className="mx-auto max-w-[1100px] space-y-8 pb-16">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">Registros de órdenes de compra</h1>
        <p className="text-sm text-slate-500">Consulta, imprime y actualiza el estado de cada OC.</p>
      </div>

      <ComprasDataStatusCard companyId={Number.isFinite(cid) ? cid : null} />

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{error}</div>
      ) : null}

      <section className="rounded-xl border border-slate-200/80 bg-white p-5 shadow-sm">
        <div className="grid gap-4 sm:grid-cols-2">
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
            <Label>Sucursal (opcional)</Label>
            <Select value={officeId} onValueChange={setOfficeId} disabled={!Number.isFinite(cid)}>
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NONE}>Todas las sucursales</SelectItem>
                {offices.map((o) => (
                  <SelectItem key={o.office_id} value={String(o.office_id)}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200/80 bg-white shadow-sm">
        <div className="overflow-x-auto p-2">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-200 hover:bg-transparent">
                <TableHead>OC</TableHead>
                <TableHead>Proveedor</TableHead>
                <TableHead>Fecha</TableHead>
                <TableHead>Estado</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="text-right">Acciones</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={6} className="py-10 text-center text-slate-500">
                    <Loader2 className="mx-auto size-6 animate-spin" />
                  </TableCell>
                </TableRow>
              ) : rows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="py-10 text-center text-slate-500">
                    {Number.isFinite(cid) ? "No hay órdenes para mostrar." : "Selecciona una empresa."}
                  </TableCell>
                </TableRow>
              ) : (
                rows.map((o) => (
                  <TableRow key={o.oc_id} className="border-slate-200">
                    <TableCell className="font-mono text-sm font-medium">#{o.oc_id}</TableCell>
                    <TableCell>{o.supplier_name ?? o.supplier_id}</TableCell>
                    <TableCell className="text-slate-600">{fmtDate(o.fecha_emision)}</TableCell>
                    <TableCell>
                      <span className="rounded-md bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-800">
                        {o.status}
                      </span>
                    </TableCell>
                    <TableCell className="text-right tabular-nums font-medium">{fmtMoney(o.total_oc)}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex flex-wrap justify-end gap-1">
                        <Button type="button" variant="outline" size="sm" onClick={() => void openDetail(o.oc_id)}>
                          <Eye className="mr-1 size-3.5" />
                          Ver
                        </Button>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={async () => {
                            if (!Number.isFinite(cid)) return
                            setDetailOpen(true)
                            setDetailLoading(true)
                            setDetailHeader(null)
                            setDetailLines([])
                            try {
                              const d = await getPurchaseOrder(o.oc_id, { companyId: cid })
                              setDetailHeader(d.header)
                              setDetailLines(d.details)
                              setAutoPrintAfterOpen(true)
                            } catch {
                              setError("No se pudo imprimir")
                              setDetailOpen(false)
                            } finally {
                              setDetailLoading(false)
                            }
                          }}
                        >
                          <Printer className="mr-1 size-3.5" />
                          Imprimir
                        </Button>
                        <Button type="button" variant="secondary" size="sm" onClick={() => openStatus(o)}>
                          Estado
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </section>

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-h-[90vh] max-w-3xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Detalle OC #{detailHeader?.oc_id}</DialogTitle>
          </DialogHeader>
          {detailLoading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="size-8 animate-spin text-slate-400" />
            </div>
          ) : detailHeader ? (
            <div id={printHostId}>
              <OcInvoicePrint header={detailHeader} details={detailLines} />
            </div>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setDetailOpen(false)}>
              Cerrar
            </Button>
            {detailHeader ? (
              <Button type="button" variant="secondary" onClick={() => triggerPrintInvoice(printHostId)}>
                <Printer className="mr-2 size-4" />
                Imprimir / PDF
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={statusOpen} onOpenChange={setStatusOpen}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>Cambiar estado</DialogTitle>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <Label>Estado de la OC #{statusOcId}</Label>
            <Select value={statusValue} onValueChange={setStatusValue}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {STATUS_OPTIONS.map((s) => (
                  <SelectItem key={s} value={s}>
                    {s}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setStatusOpen(false)} disabled={statusSaving}>
              Cancelar
            </Button>
            <Button type="button" onClick={() => void saveStatus()} disabled={statusSaving}>
              {statusSaving ? <Loader2 className="size-4 animate-spin" /> : "Guardar"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
