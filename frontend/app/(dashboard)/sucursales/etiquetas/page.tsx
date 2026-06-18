"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useSearchParams } from "next/navigation"
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
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
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
  Package,
  Tag,
  Upload,
  Search,
  Loader2,
  AlertCircle,
  Download,
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
  lookupLabelProduct,
  resolveLabelProductsBatch,
  type Company,
  type LabelProductResolved,
  type PriceListRef,
} from "@/lib/api"
import {
  estimateLabelPages,
  generateLabelsPdf,
  LABEL_FORMATS,
  type LabelFormat,
  type LabelPrintItem,
} from "@/lib/sucursales-labels-pdf"
import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"
import { resolveEtiquetasPriceList } from "@/lib/etiquetas-price-list-map"
import { markPromotionLabelGenerated } from "@/lib/promotion-labels-bridge"

type LabelRow = {
  id: string
  variantId: number
  barcode: string
  productType: string
  productName: string
  variantName: string
  displayName: string
  price: number | null
  salePrice: number | null
  regularPrice: number | null
  quantity: number
  isOffer: boolean
}

function formatCurrency(value: number | null) {
  if (value == null) return "Sin precio"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
}

function resolvedToRow(p: LabelProductResolved, quantity = 1): LabelRow {
  return {
    id: `${p.variant_id}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    variantId: p.variant_id,
    barcode: p.barcode,
    productType: p.product_type || "",
    productName: p.product_name,
    variantName: p.variant_name || "",
    displayName: p.display_name,
    price: p.price,
    salePrice: p.price,
    regularPrice: null,
    quantity,
    isOffer: false,
  }
}

type ExcelImportReview = {
  fileName: string
  totalRead: number
  duplicates: { barcode: string; count: number }[]
  notFound: {
    readBarcode: string
    quantity: number
    triedBarcodes: string[]
    error: string
  }[]
  resolved: (LabelProductResolved & { quantity: number })[]
  barcodeMappings: { read: string; matched: string }[]
}

export default function EtiquetasPage() {
  const searchParams = useSearchParams()
  const promotionPrefillDone = useRef(false)
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
  const [priceLists, setPriceLists] = useState<PriceListRef[]>([])
  const [priceListId, setPriceListId] = useState("")
  const [autoPriceList, setAutoPriceList] = useState(false)
  const [appliedPriceListName, setAppliedPriceListName] = useState("")
  const [priceListWarning, setPriceListWarning] = useState<string | null>(null)
  const [labelFormat, setLabelFormat] = useState<LabelFormat>("B")

  const [showProductType, setShowProductType] = useState(true)
  const [showBarcode, setShowBarcode] = useState(true)
  const [showPrice, setShowPrice] = useState(true)

  const [rows, setRows] = useState<LabelRow[]>([])
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
  const [excelReviewOpen, setExcelReviewOpen] = useState(false)
  const [excelReview, setExcelReview] = useState<ExcelImportReview | null>(null)

  const cid = parseInt(companyId, 10)
  const plid = parseInt(priceListId, 10)
  const configReady = Number.isFinite(cid) && cid > 0 && Number.isFinite(plid) && plid > 0

  const activeCompany = useMemo(
    () => companies.find((c) => c.company_id === cid),
    [companies, cid],
  )
  const activeCompanyName =
    activeCompany?.name ?? getStoredCompanyName() ?? "—"

  const selectedPriceListName = useMemo(
    () => priceLists.find((pl) => pl.id === plid)?.name ?? appliedPriceListName,
    [priceLists, plid, appliedPriceListName],
  )

  const totalLabels = rows.reduce((s, r) => s + r.quantity, 0)
  const estimatedPages = estimateLabelPages(totalLabels, labelFormat)

  useEffect(() => {
    getCompanies()
      .then((list) => {
        setCompanies(list)
        const urlCompany = searchParams.get("company_id")
        const stored = getStoredCompanyId()
        const fromUrl = urlCompany ? parseInt(urlCompany, 10) : NaN
        const defaultId =
          Number.isFinite(fromUrl) && list.some((c) => c.company_id === fromUrl)
            ? fromUrl
            : list.find((c) => c.company_id === stored)?.company_id ?? list[0]?.company_id
        if (defaultId != null) setCompanyId(String(defaultId))
      })
      .catch(() => setCompanies([]))
  }, [searchParams])

  useEffect(() => {
    if (!companyId) {
      setPriceLists([])
      setPriceListId("")
      setAutoPriceList(false)
      setAppliedPriceListName("")
      setPriceListWarning(null)
      return
    }
    const company = companies.find((c) => String(c.company_id) === companyId)
    const companyName = company?.name ?? getStoredCompanyName() ?? ""

    getPriceLists(cid)
      .then((lists) => {
        setPriceLists(lists)
        const resolved = resolveEtiquetasPriceList(companyName, lists)
        setPriceListId(resolved.priceListId)
        setAutoPriceList(resolved.auto)
        setAppliedPriceListName(resolved.priceListName)
        setPriceListWarning(resolved.warning)
      })
      .catch(() => {
        setPriceLists([])
        setPriceListId("")
        setAutoPriceList(false)
        setAppliedPriceListName("")
        setPriceListWarning(null)
      })
  }, [companyId, cid, companies])

  useEffect(() => {
    focusBarcodeInput()
  }, [focusBarcodeInput])

  /** Prefill desde Promociones: snapshot congelado → cola de etiquetas formato C */
  useEffect(() => {
    if (promotionPrefillDone.current) return
    if (searchParams.get("from") !== "promotion") return
    if (!configReady) return

    const barcode = (searchParams.get("barcode") || "").trim()
    if (!barcode) return

    const regularRaw = searchParams.get("regular_price")
    const saleRaw = searchParams.get("sale_price")
    const regular = regularRaw ? parseFloat(regularRaw) : NaN
    const sale = saleRaw ? parseFloat(saleRaw) : NaN
    const snapshotId = searchParams.get("snapshot_id")
    const formatParam = searchParams.get("format")

    promotionPrefillDone.current = true
    if (formatParam === "A" || formatParam === "B" || formatParam === "C") {
      setLabelFormat(formatParam)
    } else {
      setLabelFormat("C")
    }

    void lookupLabelProduct({
      company_id: cid,
      price_list_id: plid,
      barcode,
    })
      .then((product) => {
        const salePrice = Number.isFinite(sale) ? sale : product.price
        const regularPrice = Number.isFinite(regular) ? regular : null
        const row: LabelRow = {
          ...resolvedToRow(product, 1),
          isOffer: true,
          price: salePrice,
          salePrice,
          regularPrice,
        }
        setRows([row])
        if (snapshotId) {
          const sid = parseInt(snapshotId, 10)
          if (Number.isFinite(sid)) markPromotionLabelGenerated(sid)
        }
      })
      .catch(() => {
        setScanError(true)
        setScanMessage("No se pudo cargar el producto desde la promoción")
      })
  }, [searchParams, configReady, cid, plid])

  const addResolved = useCallback(
    (product: LabelProductResolved, quantity = 1) => {
      setRows((prev) => {
        const existing = prev.find((r) => r.barcode === product.barcode)
        if (existing) {
          return prev.map((r) =>
            r.barcode === product.barcode
              ? { ...r, quantity: r.quantity + quantity }
              : r,
          )
        }
        return [...prev, resolvedToRow(product, quantity)]
      })
    },
    [],
  )

  const handleScan = useCallback(async () => {
    if (scanLockRef.current) return
    const bc = barcodeInput.trim()
    if (!bc) return
    if (!configReady) {
      setScanMessage("Seleccione empresa y lista de precios")
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
      const product = await lookupLabelProduct(cid, plid, bc)
      if (!product) {
        setScanMessage(`No encontrado: ${bc}`)
        setScanError(true)
      } else {
        addResolved(product, 1)
        setScanMessage(product.display_name)
        setScanError(false)
      }
    } catch {
      setScanMessage("Error al buscar producto")
      setScanError(true)
    } finally {
      setLoading(false)
      scanLockRef.current = false
      focusBarcodeInput()
    }
  }, [barcodeInput, configReady, cid, plid, addResolved, focusBarcodeInput])

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
      if (!configReady) return
      setLoading(true)
      setSearchResults([])
      setSearchInput("")
      try {
        const product = await lookupLabelProduct(cid, plid, barcode)
        if (product) {
          addResolved(product, 1)
          setScanMessage(product.display_name)
          setScanError(false)
        } else {
          setScanMessage("No encontrado")
          setScanError(true)
        }
      } finally {
        setLoading(false)
        focusBarcodeInput()
      }
    },
    [configReady, cid, plid, addResolved, focusBarcodeInput],
  )

  const handleExcel = useCallback(
    async (file: File) => {
      if (!configReady) return
      setLoading(true)
      try {
        const buffer = await file.arrayBuffer()
        const parsed = parseEtiquetasExcel(buffer)
        if (parsed.rows.length === 0) {
          setScanMessage("Excel sin códigos válidos")
          setScanError(true)
          return
        }
        const merged = mergeEtiquetasExcelRows(parsed.rows)
        const { resolved, errors } = await resolveLabelProductsBatch(cid, plid, merged)
        const qtyByBarcode = new Map(merged.map((m) => [m.barcode, m.quantity]))
        const barcodeMappings: { read: string; matched: string }[] = []
        for (const item of resolved) {
          const read = (item.read_barcode || item.barcode).trim()
          const matched = (item.matched_barcode || item.barcode).trim()
          if (read && matched && read !== matched) {
            barcodeMappings.push({ read, matched })
          }
          for (const extra of item.extra_read_barcodes ?? []) {
            const extraRead = extra.trim()
            if (extraRead && extraRead !== matched) {
              barcodeMappings.push({ read: extraRead, matched })
            }
          }
        }
        setExcelReview({
          fileName: file.name,
          totalRead: parsed.rows.length,
          duplicates: parsed.duplicates,
          notFound: errors.map((e) => ({
            readBarcode: e.read_barcode ?? e.barcode,
            quantity: qtyByBarcode.get(e.read_barcode ?? e.barcode) ?? 1,
            triedBarcodes: e.tried_barcodes ?? [e.barcode],
            error: e.error || "Producto no encontrado",
          })),
          resolved,
          barcodeMappings,
        })
        setExcelReviewOpen(true)
        setScanError(false)
        setScanMessage(null)
      } catch (e) {
        setScanMessage(e instanceof Error ? e.message : "Error al leer Excel")
        setScanError(true)
      } finally {
        setLoading(false)
        if (fileRef.current) fileRef.current.value = ""
      }
    },
    [configReady, cid, plid],
  )

  const confirmExcelImport = useCallback(() => {
    if (!excelReview) return
    const { resolved, notFound, totalRead } = excelReview
    setRows((prev) => {
      let next = [...prev]
      for (const item of resolved) {
        const existing = next.find((r) => r.barcode === item.barcode)
        if (existing) {
          next = next.map((r) =>
            r.barcode === item.barcode
              ? { ...r, quantity: r.quantity + item.quantity }
              : r,
          )
        } else {
          next.push(resolvedToRow(item, item.quantity))
        }
      }
      return next
    })
    setScanMessage(
      `${resolved.length} producto(s) agregado(s) de ${totalRead} código(s) leído(s)${
        notFound.length ? ` · ${notFound.length} no encontrado(s)` : ""
      }`,
    )
    setScanError(resolved.length === 0)
    setExcelReviewOpen(false)
    setExcelReview(null)
    focusBarcodeInput()
  }, [excelReview, focusBarcodeInput])

  const printItems: LabelPrintItem[] = rows.map((r) => ({
    barcode: r.barcode,
    productType: r.productType,
    productName: r.productName,
    variantName: r.variantName,
    price: r.price,
    sale_price: labelFormat === "C" ? (r.salePrice ?? r.price) : r.price,
    regular_price: labelFormat === "C" ? r.regularPrice : null,
    isOffer: r.isOffer,
    quantity: r.quantity,
  }))

  const handleExportPdf = async () => {
    setPdfLoading(true)
    try {
      await generateLabelsPdf(printItems, labelFormat, {
        showProductType,
        showBarcode,
        showPrice,
      })
    } finally {
      setPdfLoading(false)
    }
  }

  const gridPreview = LABEL_FORMATS[labelFormat]

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Generador de Etiquetas</h1>
          <p className="text-sm text-muted-foreground">
            Etiquetas imprimibles en hoja carta para sucursales
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            onClick={() => setPreviewOpen(true)}
            disabled={rows.length === 0}
          >
            <FileText className="mr-2 h-4 w-4" />
            Vista previa
          </Button>
          <Button onClick={handleExportPdf} disabled={rows.length === 0 || pdfLoading}>
            {pdfLoading ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Printer className="mr-2 h-4 w-4" />
            )}
            Exportar PDF
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Configuración</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-6 lg:grid-cols-2">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2 sm:col-span-2">
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
            </div>

            <div className="space-y-2 sm:col-span-2">
              <Label>Lista de precios aplicada</Label>
              {autoPriceList ? (
                <div className="rounded-md border border-primary/25 bg-primary/5 px-3 py-2.5">
                  <p className="text-sm font-medium text-foreground">
                    {selectedPriceListName || "—"}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    Asignada automáticamente para {activeCompanyName}
                  </p>
                </div>
              ) : (
                <Select
                  value={priceListId}
                  onValueChange={(v) => {
                    setPriceListId(v)
                    const pl = priceLists.find((p) => String(p.id) === v)
                    setAppliedPriceListName(pl?.name ?? "")
                  }}
                  disabled={!companyId || priceLists.length === 0}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Seleccionar lista de precios" />
                  </SelectTrigger>
                  <SelectContent>
                    {priceLists.map((pl) => (
                      <SelectItem key={pl.id} value={String(pl.id)}>
                        {pl.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
              {priceListWarning && (
                <p className="flex items-start gap-1.5 text-xs text-amber-700">
                  <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  Seleccione la lista manualmente o revise el mapeo en consola.
                </p>
              )}
            </div>

            <div className="space-y-2 sm:col-span-2">
              <Label>Elementos visibles</Label>
              <div className="flex flex-wrap gap-4 text-sm">
                <label className="flex items-center gap-2">
                  <Switch checked={showProductType} onCheckedChange={setShowProductType} />
                  Categoría
                </label>
                <label className="flex items-center gap-2">
                  <Switch checked={showBarcode} onCheckedChange={setShowBarcode} />
                  Código barras
                </label>
                <label className="flex items-center gap-2">
                  <Switch checked={showPrice} onCheckedChange={setShowPrice} />
                  Precio
                </label>
              </div>
            </div>
          </div>

          <div className="space-y-3">
            <Label>Formato de impresión</Label>
            <RadioGroup
              value={labelFormat}
              onValueChange={(v) => setLabelFormat(v as LabelFormat)}
              className="grid gap-3"
            >
              {(Object.keys(LABEL_FORMATS) as LabelFormat[]).map((key) => {
                const f = LABEL_FORMATS[key]
                return (
                  <label
                    key={key}
                    className={`flex cursor-pointer items-start gap-3 rounded-lg border p-3 transition-colors ${
                      labelFormat === key
                        ? "border-primary bg-primary/5"
                        : "border-border hover:bg-muted/40"
                    }`}
                  >
                    <RadioGroupItem value={key} className="mt-0.5" />
                    <div>
                      <p className="font-medium">{f.shortLabel}</p>
                      <p className="text-xs text-muted-foreground">{f.description}</p>
                    </div>
                  </label>
                )
              })}
            </RadioGroup>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-blue-100 p-2">
              <Package className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{rows.length}</p>
              <p className="text-sm text-muted-foreground">Productos</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-green-100 p-2">
              <Tag className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{totalLabels}</p>
              <p className="text-sm text-muted-foreground">Etiquetas totales</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-purple-100 p-2">
              <Printer className="h-5 w-5 text-purple-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{estimatedPages}</p>
              <p className="text-sm text-muted-foreground">Hojas carta estimadas</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Escanear o buscar producto</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
            <div className="flex-1 space-y-2">
              <Label htmlFor="barcode-scan">Código de barras</Label>
              <Input
                id="barcode-scan"
                ref={barcodeRef}
                placeholder="Pistolee aquí y presione Enter"
                value={barcodeInput}
                onChange={(e) => setBarcodeInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault()
                    void handleScan()
                  }
                }}
                className="h-12 font-mono text-lg"
                autoComplete="off"
                autoFocus
              />
            </div>
            <Button size="lg" onClick={() => void handleScan()} disabled={loading}>
              {loading ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <ScanLine className="mr-2 h-4 w-4" />
              )}
              Agregar
            </Button>
          </div>

          {scanMessage && (
            <div
              className={`flex items-center gap-2 rounded-md border px-3 py-2 text-sm ${
                scanError
                  ? "border-destructive/40 bg-destructive/5 text-destructive"
                  : "border-green-200 bg-green-50 text-green-800"
              }`}
            >
              {scanError ? <AlertCircle className="h-4 w-4 shrink-0" /> : null}
              {scanMessage}
            </div>
          )}

          <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
            <div className="relative flex-1 space-y-2">
              <Label htmlFor="product-search">Buscar por nombre</Label>
              <div className="flex gap-2">
                <Input
                  id="product-search"
                  placeholder="Nombre del producto..."
                  value={searchInput}
                  onChange={(e) => setSearchInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && void handleSearch()}
                />
                <Button type="button" variant="outline" onClick={() => void handleSearch()}>
                  <Search className="h-4 w-4" />
                </Button>
              </div>
              {searchResults.length > 0 && (
                <ul className="absolute z-10 mt-1 max-h-48 w-full overflow-auto rounded-md border bg-popover shadow-md">
                  {searchResults.map((r) => (
                    <li key={r.barcode}>
                      <button
                        type="button"
                        className="w-full px-3 py-2 text-left text-sm hover:bg-muted"
                        onClick={() => void pickSearchResult(r.barcode)}
                      >
                        <span className="font-medium">{r.name}</span>
                        <span className="ml-2 font-mono text-xs text-muted-foreground">
                          {r.barcode}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div className="flex flex-wrap gap-2">
              <input
                ref={fileRef}
                type="file"
                accept=".xlsx,.xls,.csv"
                className="hidden"
                onChange={(e) => {
                  const f = e.target.files?.[0]
                  if (f) void handleExcel(f)
                }}
              />
              <Button
                type="button"
                variant="outline"
                onClick={() => downloadEtiquetasExcelTemplate()}
              >
                <Download className="mr-2 h-4 w-4" />
                Descargar plantilla Excel
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={() => fileRef.current?.click()}
                disabled={loading || !configReady}
              >
                <Upload className="mr-2 h-4 w-4" />
                Cargar Excel
              </Button>
            </div>
          </div>

          <div className="rounded-lg border bg-muted/30 p-4 text-sm">
            <p className="font-semibold">Formato esperado del Excel</p>
            <p className="text-muted-foreground mt-1">
              Columna obligatoria: <span className="font-mono text-foreground">codigo_barra</span>
              {" · "}
              Columna opcional: <span className="font-mono text-foreground">cantidad</span>
            </p>
            <div className="mt-3 overflow-x-auto rounded-md border bg-background">
              <table className="w-full min-w-[280px] text-xs">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="px-3 py-2 text-left font-semibold">codigo_barra</th>
                    <th className="px-3 py-2 text-left font-semibold">cantidad</th>
                  </tr>
                </thead>
                <tbody className="font-mono">
                  <tr className="border-b">
                    <td className="px-3 py-2">7802100505323</td>
                    <td className="px-3 py-2">1</td>
                  </tr>
                  <tr>
                    <td className="px-3 py-2">7802100001719</td>
                    <td className="px-3 py-2">1</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <ul className="text-muted-foreground mt-3 list-inside list-disc space-y-1 text-xs">
              <li>El código puede venir como texto o número.</li>
              <li>
                También se aceptan columnas: barcode, codigo, código, cod_barra, código_barra.
              </li>
              <li>
                Recomendado: formato <strong className="text-foreground">texto</strong> para evitar
                pérdida de ceros iniciales.
              </li>
              <li>Si no se informa cantidad, se genera 1 etiqueta por código.</li>
            </ul>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Cola de etiquetas</CardTitle>
            {rows.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setRows([])}
                className="text-destructive hover:text-destructive"
              >
                Limpiar lista
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {rows.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <Tag className="mb-4 h-12 w-12" />
              <p>Sin etiquetas en cola</p>
              <p className="text-sm">Escanee un código o importe un Excel</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="pb-3 pr-2">Código</th>
                    <th className="pb-3 pr-2">Producto</th>
                    <th className="pb-3 pr-2">Tipo</th>
                    <th className="pb-3 pr-2 text-right">Precio</th>
                    {labelFormat === "C" && (
                      <th className="pb-3 pr-2 text-right">Precio ref.</th>
                    )}
                    <th className="pb-3 pr-2 text-center">Cant.</th>
                    {labelFormat === "C" && (
                      <th className="pb-3 pr-2 text-center">Oferta</th>
                    )}
                    <th className="pb-3" />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.id} className="border-b last:border-0 hover:bg-muted/40">
                      <td className="py-3 pr-2 font-mono text-xs">{row.barcode}</td>
                      <td className="py-3 pr-2 font-medium">{row.displayName}</td>
                      <td className="py-3 pr-2 text-muted-foreground">{row.productType || "—"}</td>
                      <td className="py-3 pr-2 text-right font-semibold">
                        {formatCurrency(labelFormat === "C" ? row.salePrice ?? row.price : row.price)}
                      </td>
                      {labelFormat === "C" && (
                        <td className="py-3 pr-2">
                          <Input
                            type="number"
                            className="h-8 w-24 text-right text-xs"
                            placeholder="Antes"
                            value={row.regularPrice ?? ""}
                            onChange={(e) => {
                              const v = e.target.value
                              setRows((prev) =>
                                prev.map((r) =>
                                  r.id === row.id
                                    ? {
                                        ...r,
                                        regularPrice: v === "" ? null : Number(v),
                                      }
                                    : r,
                                ),
                              )
                            }}
                          />
                        </td>
                      )}
                      <td className="py-3 pr-2">
                        <div className="flex items-center justify-center gap-1">
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() =>
                              setRows((prev) =>
                                prev.map((r) =>
                                  r.id === row.id
                                    ? { ...r, quantity: Math.max(1, r.quantity - 1) }
                                    : r,
                                ),
                              )
                            }
                          >
                            −
                          </Button>
                          <span className="w-8 text-center">{row.quantity}</span>
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() =>
                              setRows((prev) =>
                                prev.map((r) =>
                                  r.id === row.id ? { ...r, quantity: r.quantity + 1 } : r,
                                ),
                              )
                            }
                          >
                            +
                          </Button>
                        </div>
                      </td>
                      {labelFormat === "C" && (
                        <td className="py-3 pr-2 text-center">
                          <Switch
                            checked={row.isOffer}
                            onCheckedChange={(checked) =>
                              setRows((prev) =>
                                prev.map((r) =>
                                  r.id === row.id
                                    ? {
                                        ...r,
                                        isOffer: checked,
                                        salePrice: r.salePrice ?? r.price,
                                      }
                                    : r,
                                ),
                              )
                            }
                          />
                        </td>
                      )}
                      <td className="py-3">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8 text-muted-foreground hover:text-destructive"
                          onClick={() => setRows((prev) => prev.filter((r) => r.id !== row.id))}
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

      <Dialog
        open={excelReviewOpen}
        onOpenChange={(open) => {
          setExcelReviewOpen(open)
          if (!open) setExcelReview(null)
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-2xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Resultado de importación Excel</DialogTitle>
            <DialogDescription>
              {excelReview?.fileName ?? "Archivo cargado"}
            </DialogDescription>
          </DialogHeader>
          {excelReview ? (
            <div className="space-y-4 text-sm">
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-md border bg-muted/30 px-3 py-2">
                  <p className="text-muted-foreground text-xs">Códigos leídos</p>
                  <p className="text-lg font-semibold">{excelReview.totalRead}</p>
                </div>
                <div className="rounded-md border bg-emerald-50 px-3 py-2">
                  <p className="text-xs text-emerald-800">Encontrados</p>
                  <p className="text-lg font-semibold text-emerald-900">
                    {excelReview.resolved.length}
                  </p>
                </div>
              </div>

              {excelReview.barcodeMappings.length > 0 ? (
                <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2">
                  <p className="font-medium text-sky-900">
                    Códigos corregidos ({excelReview.barcodeMappings.length})
                  </p>
                  <ul className="mt-2 max-h-36 space-y-1.5 overflow-y-auto text-xs text-sky-950">
                    {excelReview.barcodeMappings.map((m) => (
                      <li key={`${m.read}-${m.matched}`} className="font-mono">
                        Leído: {m.read} → encontrado como {m.matched}
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}

              {excelReview.duplicates.length > 0 ? (
                <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2">
                  <p className="font-medium text-amber-900">
                    Duplicados en el archivo ({excelReview.duplicates.length})
                  </p>
                  <ul className="mt-2 max-h-28 space-y-1 overflow-y-auto font-mono text-xs text-amber-950">
                    {excelReview.duplicates.map((d) => (
                      <li key={d.barcode}>
                        {d.barcode} · {d.count} veces
                      </li>
                    ))}
                  </ul>
                  <p className="text-muted-foreground mt-2 text-xs">
                    Las cantidades de códigos repetidos se sumarán al agregar.
                  </p>
                </div>
              ) : null}

              {excelReview.notFound.length > 0 ? (
                <div className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2">
                  <p className="font-medium text-destructive">
                    No encontrados ({excelReview.notFound.length})
                  </p>
                  <div className="mt-2 max-h-48 overflow-y-auto rounded border bg-background">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b bg-muted/40 text-left">
                          <th className="px-2 py-1.5 font-medium">Código leído</th>
                          <th className="px-2 py-1.5 font-medium">Variantes probadas</th>
                          <th className="px-2 py-1.5 font-medium">Motivo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {excelReview.notFound.map((nf) => (
                          <tr key={nf.readBarcode} className="border-b align-top">
                            <td className="px-2 py-1.5 font-mono">
                              {nf.readBarcode}
                              {nf.quantity > 1 ? (
                                <span className="text-muted-foreground block font-sans">
                                  cant. {nf.quantity}
                                </span>
                              ) : null}
                            </td>
                            <td className="px-2 py-1.5 font-mono text-[10px] leading-relaxed text-muted-foreground">
                              {nf.triedBarcodes.join(" · ")}
                            </td>
                            <td className="px-2 py-1.5">{nf.error}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}

              {excelReview.resolved.length === 0 ? (
                <p className="text-muted-foreground text-sm">
                  Ningún código del archivo existe en la lista de precios seleccionada.
                </p>
              ) : (
                <p className="text-muted-foreground text-sm">
                  Se agregarán solo los {excelReview.resolved.length} producto(s) encontrados.
                </p>
              )}
            </div>
          ) : null}
          <DialogFooter className="gap-2 sm:gap-0">
            <Button
              type="button"
              variant="outline"
              onClick={() => {
                setExcelReviewOpen(false)
                setExcelReview(null)
              }}
            >
              Cancelar
            </Button>
            <Button
              type="button"
              onClick={confirmExcelImport}
              disabled={!excelReview || excelReview.resolved.length === 0}
            >
              Agregar {excelReview?.resolved.length ?? 0} encontrados
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={previewOpen} onOpenChange={setPreviewOpen}>
        <DialogContent className="max-h-[85vh] max-w-5xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Vista previa — {LABEL_FORMATS[labelFormat].shortLabel}</DialogTitle>
            <DialogDescription>
              {gridPreview.cols}×{gridPreview.rows} = {gridPreview.perPage} etiquetas por hoja carta
            </DialogDescription>
          </DialogHeader>
          <div className="rounded-lg border bg-white p-4">
            <div
              className="grid gap-1.5"
              style={{
                gridTemplateColumns: `repeat(${gridPreview.cols}, minmax(0, 1fr))`,
              }}
            >
              {printItems
                .flatMap((item) =>
                  Array.from({ length: item.quantity }, (_, i) => ({
                    ...item,
                    key: `${item.barcode}-${i}`,
                  })),
                )
                .slice(0, gridPreview.perPage)
                .map((item) => (
                  <div
                    key={item.key}
                    className={`flex min-h-[88px] flex-col rounded border border-slate-200 bg-white p-1.5 text-left ${
                      labelFormat === "C" ? "min-h-[120px]" : ""
                    } ${labelFormat === "A" ? "min-h-[64px]" : ""}`}
                  >
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img
                      src={QUILLOTANA_LOGO_GRUPO_URL}
                      alt="Quillotana"
                      className={`mb-0.5 object-contain object-left ${
                        labelFormat === "C"
                          ? "h-5"
                          : labelFormat === "B"
                            ? "h-4"
                            : "h-3"
                      }`}
                    />
                    {labelFormat === "C" && (
                      <span className="mb-0.5 rounded bg-red-600 px-1 py-0.5 text-center text-[8px] font-bold text-white">
                        OFERTA
                      </span>
                    )}
                    {showProductType && item.productType && labelFormat !== "A" && (
                      <span className="text-[7px] font-semibold uppercase text-slate-600">
                        {item.productType}
                      </span>
                    )}
                    <p
                      className={`font-bold leading-tight text-slate-900 ${
                        labelFormat === "C"
                          ? "line-clamp-2 text-[9px]"
                          : labelFormat === "B"
                            ? "line-clamp-2 text-[8px]"
                            : "line-clamp-2 text-[7px]"
                      }`}
                    >
                      {labelFormat === "A"
                        ? [item.productName, item.variantName]
                            .filter(
                              (v, i, arr) =>
                                v &&
                                (i === 0 ||
                                  v.trim().toLowerCase() !== arr[0]?.trim().toLowerCase()),
                            )
                            .join(" ")
                        : item.productName}
                    </p>
                    {labelFormat !== "A" &&
                      item.variantName &&
                      item.variantName !== item.productName && (
                        <p className="line-clamp-1 text-[7px] text-slate-600">{item.variantName}</p>
                      )}
                    {showPrice && (
                      <div className="mt-auto pt-0.5 text-center">
                        {labelFormat === "C" &&
                        item.regular_price != null &&
                        item.sale_price != null ? (
                          <>
                            <p className="text-[7px] text-slate-400 line-through">
                              ANTES {formatCurrency(item.regular_price)}
                            </p>
                            <p className="text-[11px] font-bold text-red-600">
                              AHORA {formatCurrency(item.sale_price)}
                            </p>
                          </>
                        ) : (
                          <p
                            className={`font-bold ${
                              labelFormat === "C"
                                ? "text-[11px] text-red-600"
                                : labelFormat === "B"
                                  ? "text-[10px]"
                                  : "text-[8px]"
                            }`}
                          >
                            {formatCurrency(
                              labelFormat === "C" ? item.sale_price ?? item.price : item.price,
                            )}
                          </p>
                        )}
                      </div>
                    )}
                    {showBarcode && (
                      <div className="mt-0.5 border-t border-dashed border-slate-200 pt-0.5 text-center">
                        <div className="mx-auto mb-0.5 h-3 max-w-full bg-[repeating-linear-gradient(90deg,#000_0_2px,#fff_2px_3px)]" />
                        <p className="font-mono text-[6px] text-slate-700">{item.barcode}</p>
                      </div>
                    )}
                  </div>
                ))}
            </div>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setPreviewOpen(false)}>
              Cerrar
            </Button>
            <Button onClick={() => void handleExportPdf()} disabled={pdfLoading}>
              <Printer className="mr-2 h-4 w-4" />
              Exportar PDF
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
