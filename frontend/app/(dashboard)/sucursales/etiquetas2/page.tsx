"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  ScanLine,
  Trash2,
  Printer,
  FileText,
  Upload,
  Search,
  Loader2,
  AlertCircle,
  Download,
  Users,
} from "lucide-react"
import {
  downloadEtiquetasExcelTemplate,
  mergeEtiquetasExcelRows,
  parseEtiquetasExcel,
} from "@/lib/etiquetas-excel"
import {
  getCompanies,
  getPriceLists,
  getProductsMaster,
  getStoredCompanyId,
  getStoredCompanyName,
  type Company,
  type PriceListRef,
} from "@/lib/api"
import {
  resolveEtiquetas2PriceLists,
  type Etiquetas2ResolvedLists,
} from "@/lib/etiquetas2-price-lists"
import {
  lookupDualLabelProduct,
  resolveDualLabelProductsBatch,
  statusLabel,
  type Etiquetas2ProductRow,
  type SocioPriceStatus,
} from "@/lib/etiquetas2-dual-resolve"
import {
  estimateSocioLabelPages,
  generateSocioLabelsPdf,
  SOCIO_ESTANDAR_FORMAT,
  type SocioLabelPrintItem,
} from "@/lib/sucursales-labels-socios-pdf"
import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"

function formatCurrency(value: number | null) {
  if (value == null) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
}

function statusBadgeClass(status: SocioPriceStatus): string {
  switch (status) {
    case "OK":
      return "bg-emerald-100 text-emerald-800"
    case "SIN_PRECIO_SOCIO":
      return "bg-amber-100 text-amber-900"
    case "REVISAR_PRECIO_SOCIO":
      return "bg-red-100 text-red-800"
  }
}

export default function Etiquetas2Page() {
  const barcodeRef = useRef<HTMLInputElement>(null)
  const fileRef = useRef<HTMLInputElement>(null)
  const scanLockRef = useRef(false)

  const focusBarcodeInput = useCallback(() => {
    requestAnimationFrame(() => {
      const el = barcodeRef.current
      if (!el) return
      el.focus()
      el.select()
    })
  }, [])

  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState("")
  const [liveLists, setLiveLists] = useState<PriceListRef[]>([])
  const [listsResolved, setListsResolved] = useState<Etiquetas2ResolvedLists | null>(
    null,
  )

  const [showProductType, setShowProductType] = useState(true)
  const [showBarcode, setShowBarcode] = useState(true)
  const [showPrices, setShowPrices] = useState(true)

  const [rows, setRows] = useState<Etiquetas2ProductRow[]>([])
  const [barcodeInput, setBarcodeInput] = useState("")
  const [searchInput, setSearchInput] = useState("")
  const [searchResults, setSearchResults] = useState<
    { barcode: string; name: string }[]
  >([])
  const [scanMessage, setScanMessage] = useState<string | null>(null)
  const [scanError, setScanError] = useState(false)
  const [loading, setLoading] = useState(false)
  const [previewOpen, setPreviewOpen] = useState(false)
  const [pdfLoading, setPdfLoading] = useState(false)

  const cid = parseInt(companyId, 10)
  const configReady = Boolean(listsResolved?.configured)
  const normalListId = listsResolved?.normal?.id ?? null
  const socioListId = listsResolved?.socio?.id ?? null

  const activeCompany = useMemo(
    () => companies.find((c) => c.company_id === cid),
    [companies, cid],
  )
  const activeCompanyName =
    activeCompany?.name ?? getStoredCompanyName() ?? "—"

  const totalLabels = rows.reduce((s, r) => s + r.quantity, 0)
  const estimatedPages = estimateSocioLabelPages(totalLabels)
  const printableRows = rows.filter(
    (r) => r.normalPrice != null || r.socioPrice != null,
  )

  useEffect(() => {
    getCompanies()
      .then((list) => {
        setCompanies(list)
        const stored = getStoredCompanyId()
        const defaultId =
          list.find((c) => c.company_id === stored)?.company_id ??
          list[0]?.company_id
        if (defaultId != null) setCompanyId(String(defaultId))
      })
      .catch(() => setCompanies([]))
  }, [])

  useEffect(() => {
    if (!companyId || !Number.isFinite(cid) || cid < 1) {
      setLiveLists([])
      setListsResolved(null)
      return
    }
    const company = companies.find((c) => c.company_id === cid)
    const companyName = company?.name ?? getStoredCompanyName() ?? ""

    getPriceLists(cid)
      .then((lists) => {
        setLiveLists(lists)
        setListsResolved(resolveEtiquetas2PriceLists(cid, companyName, lists))
      })
      .catch(() => {
        setLiveLists([])
        setListsResolved({
          configured: false,
          pendingReason: "CONFIGURACIÓN DE LISTAS PENDIENTE",
          normal: null,
          socio: null,
          wow: null,
          staticLists: null,
        })
      })
  }, [companyId, cid, companies])

  useEffect(() => {
    focusBarcodeInput()
  }, [focusBarcodeInput])

  const addRow = useCallback((row: Etiquetas2ProductRow) => {
    setRows((prev) => {
      const existing = prev.find((r) => r.variantId === row.variantId)
      if (existing) {
        return prev.map((r) =>
          r.variantId === row.variantId
            ? { ...r, quantity: r.quantity + row.quantity }
            : r,
        )
      }
      return [...prev, row]
    })
  }, [])

  const handleScan = useCallback(async () => {
    if (scanLockRef.current) return
    const bc = barcodeInput.trim()
    if (!bc) return
    if (!configReady || normalListId == null || socioListId == null) {
      setScanMessage("CONFIGURACIÓN DE LISTAS PENDIENTE")
      setScanError(true)
      focusBarcodeInput()
      return
    }
    scanLockRef.current = true
    setBarcodeInput("")
    setScanMessage(null)
    setScanError(false)
    setLoading(true)
    try {
      const product = await lookupDualLabelProduct(
        cid,
        normalListId,
        socioListId,
        bc,
      )
      if (!product) {
        setScanMessage(`No encontrado: ${bc}`)
        setScanError(true)
      } else {
        addRow(product)
        setScanMessage(
          `${product.display_name} · ${statusLabel(product.status)}`,
        )
        setScanError(product.status !== "OK")
      }
    } catch {
      setScanMessage("Error al buscar producto")
      setScanError(true)
    } finally {
      setLoading(false)
      scanLockRef.current = false
      focusBarcodeInput()
    }
  }, [
    barcodeInput,
    configReady,
    normalListId,
    socioListId,
    cid,
    addRow,
    focusBarcodeInput,
  ])

  const handleSearch = useCallback(async () => {
    const term = searchInput.trim()
    if (term.length < 2) {
      setSearchResults([])
      return
    }
    try {
      const res = await getProductsMaster({ search: term, limit: 8 })
      setSearchResults(
        res.items.map((it) => ({
          barcode: it.barcode,
          name: [it.product_name, it.variant_name].filter(Boolean).join(" "),
        })),
      )
    } catch {
      setSearchResults([])
    }
  }, [searchInput])

  const pickSearchResult = useCallback(
    async (barcode: string) => {
      setSearchInput("")
      setSearchResults([])
      if (!configReady || normalListId == null || socioListId == null) {
        setScanMessage("CONFIGURACIÓN DE LISTAS PENDIENTE")
        setScanError(true)
        return
      }
      setLoading(true)
      try {
        const product = await lookupDualLabelProduct(
          cid,
          normalListId,
          socioListId,
          barcode,
        )
        if (!product) {
          setScanMessage(`No encontrado: ${barcode}`)
          setScanError(true)
        } else {
          addRow(product)
          setScanMessage(product.display_name)
          setScanError(false)
        }
      } catch {
        setScanMessage("Error al buscar producto")
        setScanError(true)
      } finally {
        setLoading(false)
        focusBarcodeInput()
      }
    },
    [configReady, normalListId, socioListId, cid, addRow, focusBarcodeInput],
  )

  const handleExcel = useCallback(
    async (file: File) => {
      if (!configReady || normalListId == null || socioListId == null) {
        setScanMessage("CONFIGURACIÓN DE LISTAS PENDIENTE")
        setScanError(true)
        return
      }
      setLoading(true)
      setScanMessage(null)
      try {
        const parsed = await parseEtiquetasExcel(file)
        const merged = mergeEtiquetasExcelRows(parsed)
        const { resolved, errors } = await resolveDualLabelProductsBatch(
          cid,
          normalListId,
          socioListId,
          merged.map((r) => ({ barcode: r.barcode, quantity: r.quantity })),
        )
        for (const row of resolved) addRow(row)
        const errN = errors.length
        setScanMessage(
          `Excel: ${resolved.length} productos` +
            (errN ? ` · ${errN} con error` : ""),
        )
        setScanError(errN > 0)
      } catch (e) {
        setScanMessage(e instanceof Error ? e.message : "Error al leer Excel")
        setScanError(true)
      } finally {
        setLoading(false)
        if (fileRef.current) fileRef.current.value = ""
        focusBarcodeInput()
      }
    },
    [
      configReady,
      normalListId,
      socioListId,
      cid,
      addRow,
      focusBarcodeInput,
    ],
  )

  const updateQty = (id: string, quantity: number) => {
    setRows((prev) =>
      prev.map((r) =>
        r.id === id ? { ...r, quantity: Math.max(1, quantity) } : r,
      ),
    )
  }

  const removeRow = (id: string) => {
    setRows((prev) => prev.filter((r) => r.id !== id))
  }

  const clearQueue = () => setRows([])

  const toPrintItems = useCallback((): SocioLabelPrintItem[] => {
    return printableRows.map((r) => ({
      barcode: r.barcode,
      productType: r.productType,
      productName: r.productName,
      variantName: r.variantName,
      normalPrice: r.normalPrice,
      socioPrice: r.socioPrice,
      quantity: r.quantity,
    }))
  }, [printableRows])

  const handlePdf = async () => {
    if (!configReady) return
    if (printableRows.length === 0) return
    setPdfLoading(true)
    try {
      await generateSocioLabelsPdf(toPrintItems(), {
        showProductType,
        showBarcode,
        showPrices,
      })
    } catch (e) {
      setScanMessage(e instanceof Error ? e.message : "Error al generar PDF")
      setScanError(true)
    } finally {
      setPdfLoading(false)
    }
  }

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <Users className="h-6 w-6 text-[#005AA8]" />
            <h1 className="text-2xl font-semibold text-foreground">
              Etiquetas Socios (Beta)
            </h1>
          </div>
          <p className="mt-1 text-sm text-muted-foreground">
            Precio Normal + Precio Socio · el generador clásico sigue en{" "}
            <a href="/sucursales/etiquetas" className="underline">
              /sucursales/etiquetas
            </a>
          </p>
        </div>
        <img
          src={QUILLOTANA_LOGO_GRUPO_URL}
          alt="Quillotana"
          className="h-10 w-auto object-contain"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Configuración</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>Empresa activa</Label>
              <Select value={companyId} onValueChange={setCompanyId}>
                <SelectTrigger>
                  <SelectValue placeholder="Seleccionar empresa" />
                </SelectTrigger>
                <SelectContent>
                  {companies.map((c) => (
                    <SelectItem key={c.company_id} value={String(c.company_id)}>
                      {c.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">{activeCompanyName}</p>
            </div>

            <div className="rounded-lg border bg-muted/30 p-3 space-y-1">
              <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Precio Normal
              </p>
              <p className="text-sm font-medium">
                {listsResolved?.normal?.name ?? "—"}
              </p>
              {listsResolved?.normal && (
                <p className="text-xs text-muted-foreground">
                  Lista ID {listsResolved.normal.id}
                </p>
              )}
            </div>

            <div className="rounded-lg border border-[#005AA8]/40 bg-[#E6F2FF]/50 p-3 space-y-1">
              <p className="text-xs font-semibold uppercase tracking-wide text-[#005AA8]">
                Precio Socio
              </p>
              <p className="text-sm font-medium">
                {listsResolved?.socio?.name ?? "—"}
              </p>
              {listsResolved?.socio?.provisional && (
                <p className="text-xs font-medium text-amber-700">
                  Provisional para pruebas
                </p>
              )}
              {listsResolved?.socio && (
                <p className="text-xs text-muted-foreground">
                  Lista ID {listsResolved.socio.id}
                </p>
              )}
            </div>

            {!configReady && (
              <div className="flex items-start gap-2 rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900">
                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                <div>
                  <p className="font-semibold">
                    CONFIGURACIÓN DE LISTAS PENDIENTE
                  </p>
                  <p className="text-xs mt-1">
                    {listsResolved?.pendingReason ||
                      "Defina Normal/Socio para esta empresa en etiquetas2-price-lists.ts"}
                  </p>
                  {liveLists.length > 0 && (
                    <p className="text-xs mt-2 text-muted-foreground">
                      Listas disponibles:{" "}
                      {liveLists.map((l) => `${l.name} (${l.id})`).join(", ")}
                    </p>
                  )}
                </div>
              </div>
            )}

            <div className="space-y-2 border-t pt-3">
              <p className="text-sm font-medium">Formato</p>
              <div className="rounded-md border p-3 text-sm">
                <p className="font-semibold">{SOCIO_ESTANDAR_FORMAT.label}</p>
                <p className="text-xs text-muted-foreground">
                  {SOCIO_ESTANDAR_FORMAT.description}
                </p>
              </div>
            </div>

            <div className="space-y-3 border-t pt-3">
              <div className="flex items-center justify-between">
                <Label htmlFor="show-type">Categoría</Label>
                <Switch
                  id="show-type"
                  checked={showProductType}
                  onCheckedChange={setShowProductType}
                />
              </div>
              <div className="flex items-center justify-between">
                <Label htmlFor="show-bc">Código de barras</Label>
                <Switch
                  id="show-bc"
                  checked={showBarcode}
                  onCheckedChange={setShowBarcode}
                />
              </div>
              <div className="flex items-center justify-between">
                <Label htmlFor="show-price">Precios</Label>
                <Switch
                  id="show-price"
                  checked={showPrices}
                  onCheckedChange={setShowPrices}
                />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Agregar productos</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Input
                ref={barcodeRef}
                placeholder="Escanear o escribir código de barras"
                value={barcodeInput}
                disabled={!configReady || loading}
                onChange={(e) => setBarcodeInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault()
                    void handleScan()
                  }
                }}
                className="min-w-[220px] flex-1"
              />
              <Button
                onClick={() => void handleScan()}
                disabled={!configReady || loading}
              >
                {loading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <ScanLine className="h-4 w-4" />
                )}
                <span className="ml-2">Agregar</span>
              </Button>
            </div>

            <div className="flex flex-wrap gap-2">
              <Input
                placeholder="Buscar por nombre…"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault()
                    void handleSearch()
                  }
                }}
                className="min-w-[220px] flex-1"
                disabled={!configReady}
              />
              <Button
                variant="secondary"
                onClick={() => void handleSearch()}
                disabled={!configReady}
              >
                <Search className="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                onClick={() => downloadEtiquetasExcelTemplate()}
              >
                <Download className="mr-2 h-4 w-4" />
                Plantilla
              </Button>
              <Button
                variant="outline"
                disabled={!configReady || loading}
                onClick={() => fileRef.current?.click()}
              >
                <Upload className="mr-2 h-4 w-4" />
                Excel
              </Button>
              <input
                ref={fileRef}
                type="file"
                accept=".xlsx,.xls"
                className="hidden"
                onChange={(e) => {
                  const f = e.target.files?.[0]
                  if (f) void handleExcel(f)
                }}
              />
            </div>

            {searchResults.length > 0 && (
              <ul className="rounded-md border divide-y max-h-40 overflow-auto text-sm">
                {searchResults.map((r) => (
                  <li key={r.barcode}>
                    <button
                      type="button"
                      className="w-full px-3 py-2 text-left hover:bg-muted"
                      onClick={() => void pickSearchResult(r.barcode)}
                    >
                      {r.name}
                      <span className="ml-2 text-xs text-muted-foreground">
                        {r.barcode}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            )}

            {scanMessage && (
              <p
                className={`text-sm ${scanError ? "text-red-600" : "text-emerald-700"}`}
              >
                {scanMessage}
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0">
          <div>
            <CardTitle>Cola / previsualización</CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              {totalLabels} etiquetas · ~{estimatedPages} hoja(s) carta ·{" "}
              {SOCIO_ESTANDAR_FORMAT.perPage}/página
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              disabled={rows.length === 0}
              onClick={clearQueue}
            >
              <Trash2 className="mr-2 h-4 w-4" />
              Vaciar
            </Button>
            <Button
              variant="secondary"
              disabled={printableRows.length === 0}
              onClick={() => setPreviewOpen(true)}
            >
              <FileText className="mr-2 h-4 w-4" />
              Vista previa
            </Button>
            <Button
              disabled={!configReady || printableRows.length === 0 || pdfLoading}
              onClick={() => void handlePdf()}
            >
              {pdfLoading ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Printer className="mr-2 h-4 w-4" />
              )}
              Generar PDF Socio
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {rows.length === 0 ? (
            <p className="text-sm text-muted-foreground py-8 text-center">
              Sin productos en cola
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="pb-2 pr-2">Producto</th>
                    <th className="pb-2 pr-2 text-right">Precio Normal</th>
                    <th className="pb-2 pr-2 text-right">Precio Socio</th>
                    <th className="pb-2 pr-2 text-center">Estado</th>
                    <th className="pb-2 pr-2 text-center">Cant.</th>
                    <th className="pb-2" />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.id} className="border-b last:border-0">
                      <td className="py-2 pr-2">
                        <div className="font-medium">{r.displayName}</div>
                        <div className="text-xs text-muted-foreground">
                          {r.barcode}
                        </div>
                      </td>
                      <td className="py-2 pr-2 text-right tabular-nums">
                        {formatCurrency(r.normalPrice)}
                      </td>
                      <td className="py-2 pr-2 text-right tabular-nums font-semibold text-[#005AA8]">
                        {formatCurrency(r.socioPrice)}
                      </td>
                      <td className="py-2 pr-2 text-center">
                        <span
                          className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusBadgeClass(r.status)}`}
                        >
                          {statusLabel(r.status)}
                        </span>
                      </td>
                      <td className="py-2 pr-2 text-center">
                        <Input
                          type="number"
                          min={1}
                          className="h-8 w-16 mx-auto text-center"
                          value={r.quantity}
                          onChange={(e) =>
                            updateQty(r.id, parseInt(e.target.value, 10) || 1)
                          }
                        />
                      </td>
                      <td className="py-2">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => removeRow(r.id)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={previewOpen} onOpenChange={setPreviewOpen}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Vista previa · Socio Estándar 10×4 cm</DialogTitle>
            <DialogDescription>
              Góndola horizontal · {SOCIO_ESTANDAR_FORMAT.perPage} etiquetas/hoja.
              &quot;Provisional&quot; no se imprime.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 sm:grid-cols-1 md:grid-cols-2">
            {printableRows.slice(0, 6).map((r) => (
              <div
                key={r.id}
                className="overflow-hidden rounded border border-neutral-200 bg-white shadow-sm"
                style={{ aspectRatio: "10 / 4" }}
              >
                <div className="flex h-full flex-col">
                  {/* Franja producto ~27.5% */}
                  <div
                    className="flex items-start gap-2 px-2.5 pt-1.5"
                    style={{ flex: "0 0 27.5%" }}
                  >
                    <img
                      src={QUILLOTANA_LOGO_GRUPO_URL}
                      alt=""
                      className="mt-0.5 h-9 w-auto shrink-0 object-contain"
                    />
                    <div className="min-w-0 flex-1 text-left leading-tight">
                      {showProductType && r.productType && (
                        <p className="truncate text-[8px] uppercase tracking-wide text-neutral-500">
                          {r.productType}
                        </p>
                      )}
                      <p className="line-clamp-2 text-[14px] font-bold text-neutral-900">
                        {r.productName}
                      </p>
                      {r.variantName &&
                        r.variantName.trim().toLowerCase() !==
                          r.productName.trim().toLowerCase() && (
                          <p className="truncate text-[10px] text-neutral-600">
                            {r.variantName}
                          </p>
                        )}
                    </div>
                  </div>

                  {/* Franja precios ~44% — 40/60 */}
                  {showPrices && (
                    <div
                      className="grid grid-cols-5 items-center"
                      style={{ flex: "0 0 43.75%" }}
                    >
                      <div className="col-span-2 flex flex-col items-center justify-center px-1">
                        <p className="text-[8px] uppercase tracking-wide text-neutral-500">
                          Precio Normal
                        </p>
                        <p className="text-[19px] font-bold tabular-nums text-neutral-900">
                          {formatCurrency(r.normalPrice)}
                        </p>
                      </div>
                      <div className="col-span-3 flex h-full items-center justify-center px-1">
                        <div className="flex w-[88%] flex-col items-center justify-center rounded-sm bg-[#F2F8FF] py-1">
                          <span className="rounded-full bg-[#005AA8] px-2 py-0.5 text-[8px] font-bold uppercase tracking-wide text-white">
                            Socio Quillotana
                          </span>
                          <p className="mt-0.5 text-[22px] font-bold tabular-nums leading-none text-[#005AA8]">
                            {formatCurrency(r.socioPrice)}
                          </p>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Franja barcode ~28.75% */}
                  {showBarcode && (
                    <div
                      className="flex flex-col items-center justify-center px-3"
                      style={{ flex: "1 1 auto" }}
                    >
                      <div
                        className="h-[14px] w-[78%] bg-[repeating-linear-gradient(90deg,#1a1a1a_0,#1a1a1a_1.2px,#fff_1.2px,#fff_2.4px)]"
                        aria-hidden
                      />
                      <p className="mt-0.5 text-[10px] tracking-wider text-neutral-800">
                        {r.barcode}
                      </p>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPreviewOpen(false)}>
              Cerrar
            </Button>
            <Button
              disabled={pdfLoading}
              onClick={() => {
                setPreviewOpen(false)
                void handlePdf()
              }}
            >
              Generar PDF
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
