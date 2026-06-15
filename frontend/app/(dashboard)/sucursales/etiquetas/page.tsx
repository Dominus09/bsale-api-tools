"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
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
  Package,
  Tag,
  Upload,
  Search,
  Loader2,
  AlertCircle,
} from "lucide-react"
import * as XLSX from "xlsx"
import {
  getCompanies,
  getPriceLists,
  getProductsMaster,
  getStoredCompanyId,
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

type LabelRow = {
  id: string
  variantId: number
  barcode: string
  productType: string
  productName: string
  variantName: string
  displayName: string
  price: number | null
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
    quantity,
    isOffer: false,
  }
}

function parseExcelRows(buffer: ArrayBuffer): { barcode: string; quantity: number }[] {
  const wb = XLSX.read(buffer, { type: "array" })
  const sheet = wb.Sheets[wb.SheetNames[0]]
  if (!sheet) return []
  const rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: "" })
  const out: { barcode: string; quantity: number }[] = []

  for (const row of rows) {
    const entries = Object.entries(row)
    let barcode = ""
    let quantity = 1
    for (const [key, raw] of entries) {
      const k = key.trim().toLowerCase()
      const val = String(raw ?? "").trim()
      if (!val) continue
      if (
        k.includes("barcode") ||
        k.includes("barras") ||
        k === "ean" ||
        k === "codigo" ||
        k === "código"
      ) {
        barcode = val
      }
      if (k.includes("cantidad") || k.includes("qty") || k === "quantity") {
        const n = parseInt(val, 10)
        if (Number.isFinite(n) && n > 0) quantity = n
      }
    }
    if (!barcode && entries.length > 0) {
      barcode = String(entries[0][1] ?? "").trim()
    }
    if (barcode) out.push({ barcode, quantity })
  }
  return out
}

export default function EtiquetasPage() {
  const barcodeRef = useRef<HTMLInputElement>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState("")
  const [priceLists, setPriceLists] = useState<PriceListRef[]>([])
  const [priceListId, setPriceListId] = useState("")
  const [labelFormat, setLabelFormat] = useState<LabelFormat>("B")

  const [showLogo, setShowLogo] = useState(true)
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

  const cid = parseInt(companyId, 10)
  const plid = parseInt(priceListId, 10)
  const configReady = Number.isFinite(cid) && cid > 0 && Number.isFinite(plid) && plid > 0

  const companyName = useMemo(
    () => companies.find((c) => c.company_id === cid)?.name || "",
    [companies, cid],
  )

  const totalLabels = rows.reduce((s, r) => s + r.quantity, 0)
  const estimatedPages = estimateLabelPages(totalLabels, labelFormat)

  useEffect(() => {
    getCompanies()
      .then((list) => {
        setCompanies(list)
        const stored = getStoredCompanyId()
        const defaultId =
          list.find((c) => c.company_id === stored)?.company_id ?? list[0]?.company_id
        if (defaultId != null) setCompanyId(String(defaultId))
      })
      .catch(() => setCompanies([]))
  }, [])

  useEffect(() => {
    if (!companyId) {
      setPriceLists([])
      setPriceListId("")
      return
    }
    getPriceLists(cid)
      .then((lists) => {
        setPriceLists(lists)
        if (lists.length > 0) setPriceListId(String(lists[0].id))
        else setPriceListId("")
      })
      .catch(() => {
        setPriceLists([])
        setPriceListId("")
      })
  }, [companyId, cid])

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
    const bc = barcodeInput.trim()
    if (!bc) return
    if (!configReady) {
      setScanMessage("Seleccione empresa y lista de precios")
      setScanError(true)
      return
    }
    setLoading(true)
    setScanMessage(null)
    setScanError(false)
    try {
      const product = await lookupLabelProduct(cid, plid, bc)
      if (!product) {
        setScanMessage("No encontrado")
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
      setBarcodeInput("")
      setLoading(false)
      barcodeRef.current?.focus()
    }
  }, [barcodeInput, configReady, cid, plid, addResolved])

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
        barcodeRef.current?.focus()
      }
    },
    [configReady, cid, plid, addResolved],
  )

  const handleExcel = useCallback(
    async (file: File) => {
      if (!configReady) return
      setLoading(true)
      try {
        const buffer = await file.arrayBuffer()
        const parsed = parseExcelRows(buffer)
        if (parsed.length === 0) {
          setScanMessage("Excel sin códigos válidos")
          setScanError(true)
          return
        }
        const { resolved, errors } = await resolveLabelProductsBatch(cid, plid, parsed)
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
          `${resolved.length} producto(s) importado(s)${
            errors.length ? `, ${errors.length} no encontrado(s)` : ""
          }`,
        )
        setScanError(errors.length > 0 && resolved.length === 0)
      } catch {
        setScanMessage("Error al leer Excel")
        setScanError(true)
      } finally {
        setLoading(false)
        if (fileRef.current) fileRef.current.value = ""
      }
    },
    [configReady, cid, plid],
  )

  const printItems: LabelPrintItem[] = rows.map((r) => ({
    barcode: r.barcode,
    productType: r.productType,
    productName: r.productName,
    variantName: r.variantName,
    price: r.price,
    isOffer: r.isOffer,
    quantity: r.quantity,
  }))

  const handleExportPdf = async () => {
    setPdfLoading(true)
    try {
      await generateLabelsPdf(printItems, labelFormat, {
        showLogo,
        showProductType,
        showBarcode,
        showPrice,
        companyName,
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
        <CardContent className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-2">
            <Label>Empresa</Label>
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
          <div className="space-y-2">
            <Label>Lista de precios</Label>
            <Select
              value={priceListId}
              onValueChange={setPriceListId}
              disabled={!companyId || priceLists.length === 0}
            >
              <SelectTrigger>
                <SelectValue placeholder="Lista de precios" />
              </SelectTrigger>
              <SelectContent>
                {priceLists.map((pl) => (
                  <SelectItem key={pl.id} value={String(pl.id)}>
                    {pl.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Formato de etiqueta</Label>
            <Select value={labelFormat} onValueChange={(v) => setLabelFormat(v as LabelFormat)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {Object.values(LABEL_FORMATS).map((f) => (
                  <SelectItem key={f.id} value={f.id}>
                    {f.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Opciones de impresión</Label>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <label className="flex items-center gap-2">
                <Switch checked={showLogo} onCheckedChange={setShowLogo} />
                Logo
              </label>
              <label className="flex items-center gap-2">
                <Switch checked={showProductType} onCheckedChange={setShowProductType} />
                Tipo
              </label>
              <label className="flex items-center gap-2">
                <Switch checked={showBarcode} onCheckedChange={setShowBarcode} />
                Barras
              </label>
              <label className="flex items-center gap-2">
                <Switch checked={showPrice} onCheckedChange={setShowPrice} />
                Precio
              </label>
            </div>
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
                disabled={loading}
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
            <div>
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
                onClick={() => fileRef.current?.click()}
                disabled={loading || !configReady}
              >
                <Upload className="mr-2 h-4 w-4" />
                Cargar Excel
              </Button>
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            Excel: columna barcode (o primera columna). Cantidad opcional. Sin cantidad → 1
            etiqueta.
          </p>
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
                    <th className="pb-3 pr-2 text-center">Cant.</th>
                    <th className="pb-3 pr-2 text-center">Oferta</th>
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
                        {formatCurrency(row.price)}
                      </td>
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
                      <td className="py-3 pr-2 text-center">
                        <Switch
                          checked={row.isOffer}
                          onCheckedChange={(checked) =>
                            setRows((prev) =>
                              prev.map((r) =>
                                r.id === row.id ? { ...r, isOffer: checked } : r,
                              ),
                            )
                          }
                        />
                      </td>
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

      <Dialog open={previewOpen} onOpenChange={setPreviewOpen}>
        <DialogContent className="max-h-[85vh] max-w-5xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Vista previa — Formato {labelFormat}</DialogTitle>
            <DialogDescription>
              {gridPreview.cols}×{gridPreview.rows} = {gridPreview.perPage} etiquetas por hoja
              carta
            </DialogDescription>
          </DialogHeader>
          <div className="rounded-lg border bg-white p-4">
            <div
              className="grid gap-2"
              style={{
                gridTemplateColumns: `repeat(${gridPreview.cols}, minmax(0, 1fr))`,
              }}
            >
              {printItems
                .flatMap((item) =>
                  Array.from({ length: item.quantity }, (_, i) => ({ ...item, key: `${item.barcode}-${i}` })),
                )
                .slice(0, gridPreview.perPage)
                .map((item) => (
                  <div
                    key={item.key}
                    className={`flex min-h-[72px] flex-col items-center justify-center rounded border border-dashed p-2 text-center ${
                      item.isOffer ? "border-red-400 bg-red-50" : ""
                    }`}
                  >
                    {item.isOffer && (
                      <Badge variant="destructive" className="mb-1 text-[10px]">
                        OFERTA
                      </Badge>
                    )}
                    {showLogo && (
                      <span className="mb-0.5 text-[8px] font-semibold text-slate-500">
                        {companyName || "Logo"}
                      </span>
                    )}
                    {showProductType && item.productType && (
                      <span className="text-[8px] uppercase text-muted-foreground">
                        {item.productType}
                      </span>
                    )}
                    <p className="line-clamp-2 text-[10px] font-semibold leading-tight">
                      {item.productName}
                    </p>
                    {showPrice && (
                      <p className="text-sm font-bold text-primary">{formatCurrency(item.price)}</p>
                    )}
                    {showBarcode && (
                      <p className="mt-1 font-mono text-[9px]">{item.barcode}</p>
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
