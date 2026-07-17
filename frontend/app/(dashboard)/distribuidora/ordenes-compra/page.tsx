"use client"

import { useCallback, useMemo, useRef, useState } from "react"
import { ChevronLeft, ChevronRight, Loader2, RefreshCw, Search } from "lucide-react"

import {
  getDistribuidoraOrdersPurchase,
  type DistribuidoraPurchaseOrder,
} from "@/lib/api"
import { FetchTimeoutError } from "@/lib/fetch-timeout"
import type { PurchaseInvoiceStatusFilter } from "@/lib/purchase-invoice-status"
import { LiveBsaleSyncPanel } from "@/components/distribuidora/orders/LiveBsaleSyncPanel"
import { OrdersFilters, type SellerOption } from "@/components/distribuidora/orders/OrdersFilters"
import { OrdersTable } from "@/components/distribuidora/orders/OrdersTable"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

const PAGE_LIMIT = 100

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

type RequestErrorKind = "timeout" | "cancelled" | "http"

type RequestError = { kind: RequestErrorKind; message: string }

const ERROR_TITLES: Record<RequestErrorKind, string> = {
  timeout: "Tiempo de espera agotado",
  cancelled: "Búsqueda cancelada",
  http: "Error",
}

export default function OrdenesCompraPage() {
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(false)
  const [invoiceStatusFilter, setInvoiceStatusFilter] =
    useState<PurchaseInvoiceStatusFilter>("all")
  const [sellerUserId, setSellerUserId] = useState("")
  const [selectedMunicipalityKeys, setSelectedMunicipalityKeys] = useState<
    ReadonlySet<string>
  >(() => new Set())
  const [selectedDeliveryDays, setSelectedDeliveryDays] = useState<
    ReadonlySet<string>
  >(() => new Set())
  const [deliveryExtraText, setDeliveryExtraText] = useState("")

  const [items, setItems] = useState<DistribuidoraPurchaseOrder[]>([])
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [nextOffset, setNextOffset] = useState<number | null>(null)
  const [hasSearched, setHasSearched] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<RequestError | null>(null)
  const [pageSelectedIds, setPageSelectedIds] = useState<Set<number>>(() => new Set())

  // Guard contra respuestas viejas: solo la búsqueda más reciente puede tocar el estado.
  const searchSeqRef = useRef(0)
  const abortRef = useRef<AbortController | null>(null)

  const deliverySearch = useMemo(() => {
    const parts = [...selectedDeliveryDays]
    if (deliveryExtraText.trim()) parts.push(deliveryExtraText.trim())
    return parts.length ? parts.join(",") : undefined
  }, [selectedDeliveryDays, deliveryExtraText])

  const municipalityCsv = useMemo(() => {
    if (selectedMunicipalityKeys.size === 0) return undefined
    return [...selectedMunicipalityKeys].join(",")
  }, [selectedMunicipalityKeys])

  const executeSearch = useCallback(
    async (offsetArg: number) => {
      const seq = ++searchSeqRef.current
      abortRef.current?.abort()
      const controller = new AbortController()
      abortRef.current = controller

      setLoading(true)
      setError(null)
      setHasSearched(true)
      try {
        const res = await getDistribuidoraOrdersPurchase({
          emission_date_from: dateFrom,
          emission_date_to: dateTo,
          only_not_invoiced: onlyNotInvoiced,
          invoice_status:
            invoiceStatusFilter === "all" ? undefined : invoiceStatusFilter,
          user_id: sellerUserId ? Number(sellerUserId) : undefined,
          delivery_search: deliverySearch,
          municipality: municipalityCsv,
          limit: PAGE_LIMIT,
          offset: offsetArg,
          signal: controller.signal,
        })
        if (seq !== searchSeqRef.current) return
        setItems(res.items)
        setOffset(offsetArg)
        setHasMore(res.has_more)
        setNextOffset(res.next_offset)
        setPageSelectedIds(new Set())
      } catch (e: unknown) {
        // Petición superada por una búsqueda más nueva: ignorar sin tocar estado.
        if (seq !== searchSeqRef.current) return
        if (e instanceof FetchTimeoutError) {
          setError({ kind: "timeout", message: e.message })
        } else if (e instanceof Error && e.name === "AbortError") {
          setError({ kind: "cancelled", message: "La búsqueda fue cancelada." })
        } else {
          setError({
            kind: "http",
            message: e instanceof Error ? e.message : "Error al cargar órdenes",
          })
        }
        setItems([])
        setHasMore(false)
        setNextOffset(null)
      } finally {
        // Solo la petición vigente apaga el loading (una vieja no pisa a la nueva).
        if (seq === searchSeqRef.current) setLoading(false)
      }
    },
    [
      dateFrom,
      dateTo,
      onlyNotInvoiced,
      invoiceStatusFilter,
      sellerUserId,
      deliverySearch,
      municipalityCsv,
    ],
  )

  const sellerOptions = useMemo((): SellerOption[] => {
    const seen = new Map<number, string>()
    for (const r of items) {
      const uid = r.user_id
      if (uid == null || !Number.isFinite(Number(uid))) continue
      const id = Number(uid)
      if (seen.has(id)) continue
      const label =
        r.seller_name?.trim() ||
        r.seller?.trim() ||
        `Usuario ${id}`
      seen.set(id, label)
    }
    return Array.from(seen.entries())
      .map(([user_id, label]) => ({ user_id, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [items])

  const municipalityOptions = useMemo(() => {
    const seen = new Map<string, string>()
    for (const r of items) {
      const k =
        (r.municipality?.trim() || r.city?.trim() || "").trim() || "__NONE__"
      const label = k === "__NONE__" ? "(Sin comuna)" : k
      seen.set(k, label)
    }
    return Array.from(seen.entries())
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [items])

  const currentPage = Math.floor(offset / PAGE_LIMIT) + 1

  return (
    <div className="mx-auto max-w-[1400px] space-y-6 p-4 md:p-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Órdenes de compra
          </h1>
          <p className="text-sm text-muted-foreground">
            Estado de facturación con folios Bsale (sin IDs internos). Confirmada
            = relateddetailid; probable = heurística operacional.
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="gap-2"
          onClick={() => void executeSearch(hasSearched ? offset : 0)}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <RefreshCw className="h-4 w-4" />
          )}
          Actualizar
        </Button>
      </div>

      {error ? (
        <Alert variant={error.kind === "cancelled" ? "default" : "destructive"}>
          <AlertTitle>{ERROR_TITLES[error.kind]}</AlertTitle>
          <AlertDescription>{error.message}</AlertDescription>
        </Alert>
      ) : null}

      <LiveBsaleSyncPanel
        onSyncComplete={() => {
          if (hasSearched) void executeSearch(offset)
        }}
      />

      <OrdersFilters
        dateFrom={dateFrom}
        onDateFromChange={setDateFrom}
        dateTo={dateTo}
        onDateToChange={setDateTo}
        selectedDeliveryDays={selectedDeliveryDays}
        onToggleDeliveryDay={(day) => {
          setSelectedDeliveryDays((prev) => {
            const next = new Set(prev)
            if (next.has(day)) next.delete(day)
            else next.add(day)
            return next
          })
        }}
        onClearDeliveryDays={() => setSelectedDeliveryDays(new Set())}
        deliveryExtraText={deliveryExtraText}
        onDeliveryExtraTextChange={setDeliveryExtraText}
        sellerOptions={sellerOptions}
        sellerUserId={sellerUserId}
        onSellerUserIdChange={setSellerUserId}
        municipalityOptions={municipalityOptions}
        selectedMunicipalityKeys={selectedMunicipalityKeys}
        onToggleMunicipality={(value) => {
          setSelectedMunicipalityKeys((prev) => {
            const next = new Set(prev)
            if (next.has(value)) next.delete(value)
            else next.add(value)
            return next
          })
        }}
        onClearMunicipalities={() => setSelectedMunicipalityKeys(new Set())}
        onlyNotInvoiced={onlyNotInvoiced}
        onOnlyNotInvoicedChange={setOnlyNotInvoiced}
        invoiceStatusFilter={invoiceStatusFilter}
        onInvoiceStatusFilterChange={setInvoiceStatusFilter}
        loading={loading}
      />

      <div className="flex flex-wrap items-center gap-3">
        <Button
          type="button"
          className="gap-2"
          onClick={() => void executeSearch(0)}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Search className="h-4 w-4" />
          )}
          Buscar
        </Button>
        <p className="text-sm text-muted-foreground">
          {loading
            ? "Cargando…"
            : !hasSearched
              ? "Presiona Buscar para cargar las órdenes del rango."
              : `${items.length} órdenes · página ${currentPage}${hasMore ? " · hay más resultados" : ""}`}
        </p>
      </div>

      <OrdersTable
        items={items}
        pageSelectedIds={pageSelectedIds}
        onToggle={(id, checked) => {
          setPageSelectedIds((prev) => {
            const next = new Set(prev)
            if (checked) next.add(id)
            else next.delete(id)
            return next
          })
        }}
        onToggleAll={(checked) => {
          if (!checked) {
            setPageSelectedIds(new Set())
            return
          }
          setPageSelectedIds(new Set(items.map((r) => r.document_id)))
        }}
        onAddToPlanning={() => {}}
        planningBasketCount={pageSelectedIds.size}
        loading={loading}
      />

      {hasSearched ? (
        <div className="flex items-center justify-end gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1"
            onClick={() => void executeSearch(Math.max(0, offset - PAGE_LIMIT))}
            disabled={loading || offset === 0}
          >
            <ChevronLeft className="h-4 w-4" />
            Anterior
          </Button>
          <span className="text-sm text-muted-foreground">Página {currentPage}</span>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-1"
            onClick={() => void executeSearch(nextOffset ?? offset + PAGE_LIMIT)}
            disabled={loading || !hasMore}
          >
            Siguiente
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      ) : null}
    </div>
  )
}
