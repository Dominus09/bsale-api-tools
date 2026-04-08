"use client"

import { type ReactNode, useCallback, useEffect, useMemo, useState } from "react"
import { FilePlus, Loader2, Pencil, Printer, Sparkles } from "lucide-react"

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
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import {
  type Company,
  type PurchaseAnalysisRow,
  type PurchaseOfficeRef,
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
const TABLE_COLS = 12

type EstadoUsuario = "COMPRAR" | "REVISAR" | "NO_COMPRAR"
type StatusFilter = "ALL" | EstadoUsuario

/** Orden de filas elegido por el usuario (sin orden fijo implícito). */
type SortByKey = "producto" | "prioridad" | "stock" | "venta"

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

/** Texto de catálogo: trim y un solo espacio entre palabras (solo presentación). */
function cleanDisplayName(s: string | null | undefined): string {
  if (s == null) return ""
  const t = String(s).trim().replace(/\s+/g, " ")
  return t
}

/** Valor mostrado en inputs numéricos: máx. 2 decimales, sin recalcular el estado interno. */
function fmtInputMax2(n: number): string {
  if (!Number.isFinite(n)) return ""
  const x = Math.round(n * 100) / 100
  if (Number.isInteger(x)) return String(x)
  const s = x.toFixed(2)
  return s.replace(/0+$/, "").replace(/\.$/, "")
}

/** Costo bruto en input: máx. 2 decimales en pantalla (mismo criterio que unidades/cajas). */
function fmtCostoInput(n: number): string {
  return fmtInputMax2(n)
}

/** Extrae N de "(SEC N)" / "SEC N" en descripción de variante (misma lógica que SQL). */
function secUnitsFromVariantText(s: string | null | undefined): number | null {
  if (s == null || !String(s).trim()) return null
  const m = String(s).toUpperCase().match(/SEC\s*(\d+)/)
  if (!m) return null
  const n = parseInt(m[1], 10)
  return Number.isFinite(n) && n > 0 ? n : null
}

/**
 * CxC: bsale.variants.units_per_box vía API (vw_purchase_analysis ya resuelve SEC en description si columna NULL/0).
 * Si aún falta, mismo respaldo SEC en cliente para no mostrar 1 por defecto cuando hay texto.
 */
function cxcFromAnalysisRow(r: PurchaseAnalysisRow): number {
  const v = r.units_per_box
  if (v != null && Number(v) > 0) return Number(v)
  const sec = secUnitsFromVariantText(r.variant_name)
  if (sec != null) return sec
  return 1
}

function NameCellWithTooltip({
  raw,
  className,
}: {
  raw: string | null | undefined
  className?: string
}) {
  const c = cleanDisplayName(raw) || "—"
  const body = (
    <span className={cn("line-clamp-3 whitespace-normal break-words text-sm leading-snug", className)}>
      {c}
    </span>
  )
  if (c === "—") {
    return body
  }
  return (
    <Tooltip delayDuration={300}>
      <TooltipTrigger asChild>
        <button
          type="button"
          className="w-full cursor-default text-left outline-none focus-visible:ring-2 focus-visible:ring-slate-400/60 rounded-sm"
        >
          {body}
        </button>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="max-w-md whitespace-pre-wrap text-left font-normal">
        {c}
      </TooltipContent>
    </Tooltip>
  )
}

function effectiveUpb(stored: number | undefined): number {
  return stored != null && Number.isFinite(stored) && stored > 0 ? stored : 1
}

function roundMax2(n: number): number {
  if (!Number.isFinite(n)) return 0
  return Math.round(n * 100) / 100
}

/** Solo cajas completas: CEIL(unidades/upb), unidades finales = cajas * upb. */
function unitsToWholeBoxes(unitsRaw: number, upb: number): number {
  const box = upb > 0 && Number.isFinite(upb) ? upb : 1
  const u = Math.max(0, roundMax2(unitsRaw))
  if (u <= 0) return 0
  const cajas = Math.ceil(u / box)
  return cajas * box
}

/** Cajas = unidades / CxC; entero si cae justo, si no hasta 2 decimales. */
function fmtCajasDisplay(qty: number, upb: number): string {
  const b = effectiveUpb(upb)
  if (qty <= 0) return "0"
  const c = qty / b
  if (Number.isInteger(c)) return String(c)
  return fmtInputMax2(c)
}

function parseCajasDecimal(raw: string): number | null {
  const t = String(raw).replace(",", ".").trim()
  if (t === "") return null
  const n = parseFloat(t)
  if (!Number.isFinite(n) || n < 0) return null
  return n
}

/** Menos de ~7 días de stock según promedio diario → prioridad en la tabla. */
function isCriticalRow(r: PurchaseAnalysisRow): boolean {
  const stock = Number(r.stock_actual) || 0
  if (stock <= 0) return true
  const pd = Number(r.promedio_diario) || 0
  if (pd <= 0) return false
  return stock / pd < 7
}

function estadoUsuarioForAnalysis(qty: number, touched: boolean): EstadoUsuario {
  const q = roundMax2(qty)
  if (q <= 0) return "NO_COMPRAR"
  if (touched) return "REVISAR"
  return "COMPRAR"
}

/**
 * Sugerencia 14 días: promedio_diario = ventas_30/30, demanda_14 = promedio*14,
 * crudo = max(demanda_14 - stock, 0), luego redondeo a cajas completas.
 */
function suggestedUnitsFrom14dCoverage(r: PurchaseAnalysisRow, upb: number): number {
  const v30 = Number(r.ventas_30_dias) || 0
  const stock = Number(r.stock_actual) || 0
  const prom = v30 / 30
  const demand14 = prom * 14
  const raw = Math.max(0, roundMax2(demand14 - stock))
  return unitsToWholeBoxes(raw, upb)
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

type DisplayRow =
  | { kind: "manual"; key: string; m: ManualDraft }
  | { kind: "analysis"; key: string; r: PurchaseAnalysisRow }

function productNameOf(item: DisplayRow): string {
  return (item.kind === "manual" ? item.m.product_name : item.r.product_name) || ""
}

/**
 * Prioridad: CRÍTICO → BAJO (no comprar) → COMPRAR → resto (p. ej. REVISAR).
 */
function prioridadSortRank(item: DisplayRow, estado: EstadoUsuario): number {
  if (item.kind === "analysis" && isCriticalRow(item.r)) return 0
  if (estado === "NO_COMPRAR") return 1
  if (estado === "COMPRAR") return 2
  return 3
}

function stockSortValue(item: DisplayRow): number {
  if (item.kind === "manual") return Number.POSITIVE_INFINITY
  return Number(item.r.stock_actual) || 0
}

/** Ventas 30 días; manuales quedan al final al ordenar por venta (mayor a menor). */
function ventaSortValue(item: DisplayRow): number {
  if (item.kind === "manual") return Number.NEGATIVE_INFINITY
  return Number(item.r.ventas_30_dias) || 0
}

function manualEffectiveUpb(m: ManualDraft): number {
  return m.units_per_box != null && m.units_per_box > 0 ? m.units_per_box : 1
}

function statusRowClass(status: EstadoUsuario | string): string {
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

function EstadoUsuarioBadge({ estado }: { estado: EstadoUsuario }) {
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-xs font-medium",
        estado === "COMPRAR" && "bg-emerald-600/25 text-emerald-900",
        estado === "REVISAR" && "bg-amber-500/30 text-amber-950",
        estado === "NO_COMPRAR" && "bg-red-600/25 text-red-900",
      )}
    >
      {estado}
    </span>
  )
}

/** Fila con tooltip si solo se marcaron cambios de cantidad/cajas (sin columna extra). */
function PurchaseLineTableRow({
  qtyEdited,
  className,
  children,
}: {
  qtyEdited: boolean
  className?: string
  children: ReactNode
}) {
  const row = <TableRow className={className}>{children}</TableRow>
  if (!qtyEdited) return row
  return (
    <Tooltip delayDuration={400}>
      <TooltipTrigger asChild>{row}</TooltipTrigger>
      <TooltipContent side="left" align="center" className="max-w-[16rem] text-xs font-normal">
        Cantidad modificada manualmente
      </TooltipContent>
    </Tooltip>
  )
}

export default function GenerarOcPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<PurchaseOfficeRef[]>([])
  const [suppliers, setSuppliers] = useState<Supplier[]>([])

  const [companyId, setCompanyId] = useState<string>(NONE)
  const [officeId, setOfficeId] = useState<string>(NONE)
  const [supplierId, setSupplierId] = useState<string>(NONE)

  const [analysis, setAnalysis] = useState<PurchaseAnalysisRow[]>([])
  const [loadingAnalysis, setLoadingAnalysis] = useState(false)
  const [error, setError] = useState("")

  const [qtyByKey, setQtyByKey] = useState<Record<string, number>>({})
  const [costoByKey, setCostoByKey] = useState<Record<string, number>>({})
  const [upbByKey, setUpbByKey] = useState<Record<string, number>>({})
  /** Filas de análisis editadas por el usuario tras la última carga o «Sugerir compra» → estado_usuario REVISAR. */
  const [analysisTouchedKeys, setAnalysisTouchedKeys] = useState<Set<string>>(() => new Set())
  /** Solo cambios en unidades o cajas (indicador visual + tooltip; no columna nueva). */
  const [quantityEditedKeys, setQuantityEditedKeys] = useState<Set<string>>(() => new Set())
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL")
  const [sortBy, setSortBy] = useState<SortByKey>("producto")
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

  const markAnalysisTouched = useCallback((key: string) => {
    setAnalysisTouchedKeys((prev) => new Set(prev).add(key))
  }, [])

  const markQuantityEdited = useCallback((key: string) => {
    setQuantityEditedKeys((prev) => new Set(prev).add(key))
  }, [])

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
    setCostoByKey({})
    setUpbByKey({})
    setAnalysisTouchedKeys(new Set())
    setQuantityEditedKeys(new Set())
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
      const c: Record<string, number> = {}
      const u: Record<string, number> = {}
      for (const r of rows) {
        const k = rowKey(r)
        const upb0 = cxcFromAnalysisRow(r)
        u[k] = upb0
        q[k] = unitsToWholeBoxes(Number(r.unidades_a_comprar) || 0, upb0)
        c[k] = Number(r.costo_bruto) || 0
      }
      setQtyByKey(q)
      setCostoByKey(c)
      setUpbByKey(u)
      setAnalysisTouchedKeys(new Set())
      setQuantityEditedKeys(new Set())
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
      setCostoByKey({})
      setUpbByKey({})
      setAnalysisTouchedKeys(new Set())
      setQuantityEditedKeys(new Set())
      setManualRows([])
    }
  }, [filtersReady, loadAnalysis])

  const updateUnidadesAnalysis = (key: string, raw: string) => {
    const n = parseFloat(raw.replace(",", "."))
    if (!Number.isFinite(n) || n < 0) return
    setQtyByKey((prev) => ({
      ...prev,
      [key]: roundMax2(n),
    }))
    markAnalysisTouched(key)
    markQuantityEdited(key)
  }

  const updateCajasAnalysis = (key: string, raw: string) => {
    const cajas = parseCajasDecimal(raw)
    if (cajas === null) return
    const upb = effectiveUpb(upbByKey[key])
    setQtyByKey((prev) => ({
      ...prev,
      [key]: roundMax2(cajas * upb),
    }))
    markAnalysisTouched(key)
    markQuantityEdited(key)
  }

  const updateCostoAnalysis = (key: string, raw: string) => {
    const n = parseFloat(raw.replace(",", "."))
    setCostoByKey((prev) => ({
      ...prev,
      [key]: Number.isFinite(n) && n >= 0 ? n : 0,
    }))
    markAnalysisTouched(key)
  }

  const updateUpbAnalysis = (key: string, raw: string) => {
    let n = parseInt(raw.replace(/\D/g, ""), 10)
    if (!Number.isFinite(n) || n <= 0) n = 1
    setUpbByKey((prev) => ({ ...prev, [key]: n }))
    setQtyByKey((prev) => ({
      ...prev,
      [key]: roundMax2(prev[key] ?? 0),
    }))
    markAnalysisTouched(key)
  }

  const patchManual = (key: string, fn: (m: ManualDraft) => ManualDraft) => {
    setManualRows((rows) => rows.map((r) => (r.key === key ? fn(r) : r)))
  }

  const updateManualUnidades = (key: string, raw: string) => {
    const n = parseFloat(raw.replace(",", "."))
    if (!Number.isFinite(n) || n < 0) return
    patchManual(key, (m) => ({
      ...m,
      cantidad: roundMax2(n),
    }))
    markQuantityEdited(key)
  }

  const updateManualCajas = (key: string, raw: string) => {
    const cajas = parseCajasDecimal(raw)
    if (cajas === null) return
    patchManual(key, (m) => ({
      ...m,
      cantidad: roundMax2(cajas * manualEffectiveUpb(m)),
    }))
    markQuantityEdited(key)
  }

  const updateManualCosto = (key: string, raw: string) => {
    const n = parseFloat(raw.replace(",", "."))
    if (!Number.isFinite(n) || n < 0) return
    patchManual(key, (m) => ({ ...m, costo_bruto: n }))
  }

  const updateManualUpb = (key: string, raw: string) => {
    let n = parseInt(raw.replace(/\D/g, ""), 10)
    if (!Number.isFinite(n) || n <= 0) n = 1
    patchManual(key, (m) => ({
      ...m,
      units_per_box: n,
      cantidad: roundMax2(m.cantidad),
    }))
  }

  const applySuggestCompra = useCallback(() => {
    if (!filtersReady || analysis.length === 0) return
    setAnalysisTouchedKeys((prev) => {
      const next = new Set(prev)
      for (const r of analysis) next.delete(rowKey(r))
      return next
    })
    setQuantityEditedKeys((prev) => {
      const next = new Set(prev)
      for (const r of analysis) next.delete(rowKey(r))
      return next
    })
    setQtyByKey((prev) => {
      const next = { ...prev }
      for (const r of analysis) {
        const k = rowKey(r)
        const upb = effectiveUpb(upbByKey[k] ?? cxcFromAnalysisRow(r))
        next[k] = suggestedUnitsFrom14dCoverage(r, upb)
      }
      return next
    })
  }, [filtersReady, analysis, upbByKey])

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
      cantidad: roundMax2(qty),
    }
    setManualRows((prev) => [...prev, row])
    setError("")
    setAddOpen(false)
  }

  const removeManual = (key: string) => {
    setQuantityEditedKeys((prev) => {
      const next = new Set(prev)
      next.delete(key)
      return next
    })
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
      const qtyRaw = qtyByKey[k] ?? 0
      const upb = effectiveUpb(upbByKey[k])
      const qty = roundMax2(qtyRaw)
      if (qty <= 0) continue
      out.push({
        variant_id: r.variant_id,
        product_type_name: r.product_type_name,
        product_name: r.product_name,
        variant_name: r.variant_name,
        barcode: r.barcode,
        cantidad: qty,
        units_per_box: upb,
        costo_unitario: costoByKey[k] ?? 0,
      })
    }
    for (const m of manualRows) {
      const mupb = manualEffectiveUpb(m)
      const mcant = roundMax2(m.cantidad)
      if (mcant <= 0) continue
      out.push({
        variant_id: null,
        product_type_name: m.product_type_name || null,
        product_name: m.product_name,
        variant_name: m.variant_name || null,
        barcode: m.barcode || null,
        cantidad: mcant,
        units_per_box: mupb,
        costo_unitario: m.costo_bruto,
      })
    }
    return out
  }, [analysis, qtyByKey, costoByKey, upbByKey, manualRows, filtersReady])

  const orderSummary = useMemo(() => {
    let total = 0
    let cajas = 0
    let productos = 0
    for (const line of linesToSubmit) {
      const upb = line.units_per_box != null && line.units_per_box > 0 ? line.units_per_box : 1
      total += line.cantidad * line.costo_unitario
      cajas += line.cantidad / upb
      productos += 1
    }
    return { total, cajas, productos }
  }, [linesToSubmit])

  const displayRows = useMemo(() => {
    const items: DisplayRow[] = [
      ...manualRows.map((m) => ({ kind: "manual" as const, key: m.key, m })),
      ...analysis.map((r) => ({ kind: "analysis" as const, key: rowKey(r), r })),
    ]

    const estadoOf = (item: DisplayRow): EstadoUsuario => {
      if (item.kind === "manual") {
        return roundMax2(item.m.cantidad) > 0 ? "COMPRAR" : "NO_COMPRAR"
      }
      const qty = qtyByKey[item.key] ?? 0
      return estadoUsuarioForAnalysis(qty, analysisTouchedKeys.has(item.key))
    }

    const filtered = items.filter((item) => {
      if (statusFilter === "ALL") return true
      return estadoOf(item) === statusFilter
    })

    filtered.sort((a, b) => {
      const ea = estadoOf(a)
      const eb = estadoOf(b)
      const na = productNameOf(a)
      const nb = productNameOf(b)
      let cmp = 0
      switch (sortBy) {
        case "producto":
          cmp = na.localeCompare(nb, "es")
          break
        case "prioridad": {
          cmp = prioridadSortRank(a, ea) - prioridadSortRank(b, eb)
          if (cmp === 0) cmp = na.localeCompare(nb, "es")
          break
        }
        case "stock": {
          cmp = stockSortValue(a) - stockSortValue(b)
          if (!Number.isFinite(cmp) || cmp === 0) cmp = na.localeCompare(nb, "es")
          break
        }
        case "venta": {
          cmp = ventaSortValue(b) - ventaSortValue(a)
          if (!Number.isFinite(cmp) || cmp === 0) cmp = na.localeCompare(nb, "es")
          break
        }
        default:
          cmp = na.localeCompare(nb, "es")
      }
      return cmp
    })

    return filtered
  }, [manualRows, analysis, qtyByKey, analysisTouchedKeys, statusFilter, sortBy])

  const openConfirm = () => {
    if (!filtersReady) {
      setError("Selecciona empresa, sucursal y proveedor")
      return
    }
    if (linesToSubmit.length === 0) {
      setError("No hay líneas para la orden (revisa cantidades mayores a cero)")
      return
    }
    setError("")
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
    <div className="mx-auto w-full max-w-[min(100%,1680px)] space-y-6 pb-16">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">Generar orden de compra</h1>
        <p className="text-sm text-slate-500">
          Filtros y líneas de compra; la confirmación (fecha, pago, responsable) se hace en el paso final.
        </p>
      </div>

      <ComprasDataStatusCard companyId={Number.isFinite(cid) ? cid : null} />

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{error}</div>
      ) : null}

      <section className="rounded-xl border border-slate-200/80 bg-white p-5 shadow-sm">
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
      </section>

      <div className="flex flex-wrap items-center gap-3">
        <Button type="button" variant="outline" onClick={openAddManual} disabled={!filtersReady}>
          <FilePlus className="mr-2 size-4" />
          Agregar producto
        </Button>
        <Button
          type="button"
          variant="secondary"
          onClick={applySuggestCompra}
          disabled={!filtersReady || loadingAnalysis || analysis.length === 0}
        >
          <Sparkles className="mr-2 size-4" />
          Sugerir compra
        </Button>
        <Button type="button" onClick={openConfirm} disabled={!filtersReady || loadingAnalysis}>
          Generar orden de compra
        </Button>
        <div className="flex min-w-[12rem] flex-1 flex-wrap items-center gap-2 sm:flex-initial">
          <Label className="whitespace-nowrap text-xs text-slate-600">Filtrar estado</Label>
          <Select
            value={statusFilter}
            onValueChange={(v) => setStatusFilter(v as StatusFilter)}
            disabled={!filtersReady}
          >
            <SelectTrigger className="h-9 w-[11rem] bg-white">
              <SelectValue placeholder="Estado" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">Todos</SelectItem>
              <SelectItem value="COMPRAR">COMPRAR</SelectItem>
              <SelectItem value="REVISAR">REVISAR</SelectItem>
              <SelectItem value="NO_COMPRAR">NO_COMPRAR</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="flex min-w-[12rem] flex-wrap items-center gap-2">
          <Label className="whitespace-nowrap text-xs text-slate-600">Ordenar por</Label>
          <Select
            value={sortBy}
            onValueChange={(v) => setSortBy(v as SortByKey)}
            disabled={!filtersReady}
          >
            <SelectTrigger className="h-9 w-[10.5rem] bg-white">
              <SelectValue placeholder="Orden" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="producto">Producto</SelectItem>
              <SelectItem value="prioridad">Prioridad</SelectItem>
              <SelectItem value="stock">Stock</SelectItem>
              <SelectItem value="venta">Venta</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {loadingAnalysis ? (
          <span className="inline-flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="size-4 animate-spin" />
            Cargando sugerencias…
          </span>
        ) : null}
      </div>

      <section className="rounded-xl border border-slate-200/80 bg-white shadow-sm">
        <div className="border-b border-slate-100 px-5 py-3">
          <h2 className="text-sm font-medium text-slate-800">Líneas</h2>
          <p className="text-xs text-slate-500">
            El estado mostrado es el <span className="font-medium text-slate-600">estado_usuario</span> (recalculado
            aquí). El backend sigue enviando <span className="font-medium text-slate-600">estado_sistema</span> en el
            campo API <span className="font-mono text-slate-700">status</span> (no se muestra).{" "}
            <span className="font-medium text-slate-600">
              Sugerir compra: cobertura 14 días y cajas completas. Edición manual: permite unidades fraccionadas respecto
              de CxC.
            </span>
          </p>
        </div>
        <div className="grid gap-3 border-b border-slate-100 px-5 py-3 sm:grid-cols-3">
          <div>
            <p className="text-xs text-slate-500">Total compra (líneas con cantidad &gt; 0)</p>
            <p className="text-lg font-semibold tabular-nums text-slate-900">
              ${fmtNum(orderSummary.total, 0)}
            </p>
          </div>
          <div>
            <p className="text-xs text-slate-500">Total cajas (equiv.)</p>
            <p className="text-lg font-semibold tabular-nums text-slate-900">
              {fmtNum(orderSummary.cajas, 2)}
            </p>
          </div>
          <div>
            <p className="text-xs text-slate-500">Productos en OC</p>
            <p className="text-lg font-semibold tabular-nums text-slate-900">{orderSummary.productos}</p>
          </div>
        </div>
        <div className="max-h-[min(72vh,820px)] min-w-0 overflow-auto">
          <table className="w-full min-w-[1180px] border-collapse text-sm">
            <TableHeader className="sticky top-0 z-30 border-b border-slate-200 bg-white shadow-[0_1px_0_0_rgb(226_232_240)] [&_tr]:border-slate-200 [&_tr]:hover:bg-transparent">
              <TableRow>
                <TableHead className="bg-white">Estado</TableHead>
                <TableHead className="min-w-[7rem] bg-white">Tipo</TableHead>
                <TableHead className="min-w-[10rem] max-w-[min(320px,40vw)] bg-white">Producto</TableHead>
                <TableHead className="min-w-[12rem] max-w-[min(480px,52vw)] bg-white">Variante</TableHead>
                <TableHead className="bg-white text-right">Stock</TableHead>
                <TableHead className="bg-white text-right whitespace-nowrap">7 días / 30 días</TableHead>
                <TableHead className="bg-white text-right">Costo bruto</TableHead>
                <TableHead className="bg-white text-right" title="Cantidad por caja (units_per_box)">
                  CxC
                </TableHead>
                <TableHead className="bg-white text-right">Unidades</TableHead>
                <TableHead className="bg-white text-right">Cajas</TableHead>
                <TableHead className="bg-white text-right tabular-nums">Total</TableHead>
                <TableHead className="w-[4.5rem] bg-white" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {analysis.length === 0 && manualRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={TABLE_COLS} className="py-12 text-center text-slate-500">
                    {filtersReady
                      ? "No hay filas para este contexto."
                      : "Elige empresa, sucursal y proveedor para ver sugerencias."}
                  </TableCell>
                </TableRow>
              ) : null}
              {filtersReady &&
              analysis.length + manualRows.length > 0 &&
              displayRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={TABLE_COLS} className="py-10 text-center text-slate-500">
                    Ninguna fila coincide con el filtro de estado. Cambia el filtro o revisa cantidades.
                  </TableCell>
                </TableRow>
              ) : null}
              {displayRows.map((item) => {
                if (item.kind === "manual") {
                  const m = item.m
                  const upb = manualEffectiveUpb(m)
                  const estado: EstadoUsuario = roundMax2(m.cantidad) > 0 ? "COMPRAR" : "NO_COMPRAR"
                  const total = m.cantidad * m.costo_bruto
                  const qtyEdited = quantityEditedKeys.has(item.key)
                  return (
                    <PurchaseLineTableRow
                      key={item.key}
                      qtyEdited={qtyEdited}
                      className={cn(
                        "border-slate-200 bg-sky-500/10",
                        statusRowClass(estado),
                        qtyEdited && "border-l-[3px] border-l-sky-600/80 bg-sky-500/[0.14]",
                      )}
                    >
                      <TableCell className="align-top">
                        <div className="flex flex-col gap-1">
                          <div className="flex flex-wrap items-center gap-1">
                            <EstadoUsuarioBadge estado={estado} />
                            {qtyEdited ? (
                              <Pencil
                                className="size-3.5 shrink-0 text-sky-800/85"
                                aria-hidden
                              />
                            ) : null}
                          </div>
                          <span className="text-[10px] font-medium uppercase tracking-wide text-sky-800">
                            Manual
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="max-w-[min(280px,36vw)] whitespace-normal align-top text-slate-600">
                        <NameCellWithTooltip raw={m.product_type_name} />
                      </TableCell>
                      <TableCell className="max-w-[min(320px,40vw)] whitespace-normal align-top">
                        <NameCellWithTooltip raw={m.product_name} className="font-medium text-slate-900" />
                      </TableCell>
                      <TableCell className="max-w-[min(480px,52vw)] whitespace-normal align-top text-slate-600">
                        <NameCellWithTooltip raw={m.variant_name} />
                      </TableCell>
                      <TableCell className="text-right">—</TableCell>
                      <TableCell className="text-right">—</TableCell>
                      <TableCell className="text-right align-middle">
                        <Input
                          className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                          inputMode="decimal"
                          value={fmtCostoInput(m.costo_bruto)}
                          onChange={(e) => updateManualCosto(m.key, e.target.value)}
                        />
                      </TableCell>
                      <TableCell className="text-right align-middle">
                        <Input
                          className="ml-auto h-8 w-14 tabular-nums text-right"
                          inputMode="numeric"
                          value={String(upb)}
                          onChange={(e) => updateManualUpb(m.key, e.target.value)}
                        />
                      </TableCell>
                      <TableCell className="text-right align-middle">
                        <Input
                          className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                          inputMode="decimal"
                          value={fmtInputMax2(m.cantidad)}
                          onChange={(e) => updateManualUnidades(m.key, e.target.value)}
                        />
                      </TableCell>
                      <TableCell className="text-right align-middle">
                        <Input
                          className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                          inputMode="decimal"
                          value={fmtCajasDisplay(m.cantidad, upb)}
                          onChange={(e) => updateManualCajas(m.key, e.target.value)}
                        />
                      </TableCell>
                      <TableCell className="text-right tabular-nums font-medium align-middle">
                        {fmtNum(total, 0)}
                      </TableCell>
                      <TableCell className="align-middle">
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="text-red-600"
                          onClick={() => removeManual(m.key)}
                        >
                          Quitar
                        </Button>
                      </TableCell>
                    </PurchaseLineTableRow>
                  )
                }
                const r = item.r
                const k = item.key
                const qty = qtyByKey[k] ?? 0
                const upb = effectiveUpb(upbByKey[k])
                const unit = costoByKey[k] ?? 0
                const total = qty * unit
                const estado = estadoUsuarioForAnalysis(qty, analysisTouchedKeys.has(k))
                const qtyEdited = quantityEditedKeys.has(k)
                return (
                  <PurchaseLineTableRow
                    key={k}
                    qtyEdited={qtyEdited}
                    className={cn(
                      "border-slate-200",
                      statusRowClass(estado),
                      qtyEdited && "border-l-[3px] border-l-sky-600/80 bg-slate-50/90",
                    )}
                  >
                    <TableCell className="align-top">
                      <div className="flex flex-wrap items-center gap-1">
                        <EstadoUsuarioBadge estado={estado} />
                        {qtyEdited ? (
                          <Pencil className="size-3.5 shrink-0 text-sky-800/85" aria-hidden />
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell className="min-w-[7rem] whitespace-normal align-top text-slate-600">
                      <NameCellWithTooltip raw={r.product_type_name} />
                    </TableCell>
                    <TableCell className="max-w-[min(320px,40vw)] whitespace-normal align-top">
                      <NameCellWithTooltip raw={r.product_name} className="font-medium text-slate-900" />
                    </TableCell>
                    <TableCell className="max-w-[min(480px,52vw)] whitespace-normal align-top text-slate-600">
                      <NameCellWithTooltip raw={r.variant_name} />
                    </TableCell>
                    <TableCell className="text-right tabular-nums align-middle">{fmtNum(r.stock_actual, 0)}</TableCell>
                    <TableCell className="text-right tabular-nums text-xs text-slate-600 align-middle">
                      {fmtNum(r.ventas_7_dias, 0)} / {fmtNum(r.ventas_30_dias, 0)}
                    </TableCell>
                    <TableCell className="text-right align-middle">
                      <Input
                        className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                        inputMode="decimal"
                        value={fmtCostoInput(unit)}
                        onChange={(e) => updateCostoAnalysis(k, e.target.value)}
                      />
                    </TableCell>
                    <TableCell className="text-right align-middle">
                      <Input
                        className="ml-auto h-8 w-14 tabular-nums text-right"
                        inputMode="numeric"
                        value={String(upb)}
                        onChange={(e) => updateUpbAnalysis(k, e.target.value)}
                      />
                    </TableCell>
                    <TableCell className="text-right align-middle">
                      <Input
                        className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                        inputMode="decimal"
                        value={fmtInputMax2(qty)}
                        onChange={(e) => updateUnidadesAnalysis(k, e.target.value)}
                      />
                    </TableCell>
                    <TableCell className="text-right align-middle">
                      <Input
                        className="ml-auto h-8 w-[6.5rem] tabular-nums text-right"
                        inputMode="decimal"
                        value={fmtCajasDisplay(qty, upb)}
                        onChange={(e) => updateCajasAnalysis(k, e.target.value)}
                      />
                    </TableCell>
                    <TableCell className="text-right tabular-nums font-medium text-slate-900 align-middle">
                      {fmtNum(total, 0)}
                    </TableCell>
                    <TableCell />
                  </PurchaseLineTableRow>
                )
              })}
            </TableBody>
          </table>
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
        <DialogContent className="flex h-[min(94vh,calc(100dvh-1rem))] max-h-[min(94vh,calc(100dvh-1rem))] w-[calc(100vw-0.75rem)] max-w-[calc(100vw-0.75rem)] flex-col gap-3 overflow-hidden p-3 sm:h-auto sm:max-h-[min(94vh,960px)] sm:w-[min(96vw,1400px)] sm:max-w-[min(96vw,1400px)] sm:gap-4 sm:p-5">
          <DialogHeader className="shrink-0 space-y-1">
            <DialogTitle>Orden generada</DialogTitle>
          </DialogHeader>
          {resultHeader ? (
            <div
              id={printHostId}
              className="min-h-0 min-w-0 flex-1 overflow-y-auto overflow-x-auto sm:overflow-x-hidden rounded-md border border-slate-200/80 bg-slate-50/50 p-2 sm:p-3 print:overflow-visible print:border-0 print:bg-transparent print:p-0"
            >
              <OcInvoicePrint header={resultHeader} details={resultDetails} />
            </div>
          ) : null}
          <DialogFooter className="shrink-0 gap-2 sm:justify-end">
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
