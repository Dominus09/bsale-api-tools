"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import {
  getDistribuidoraOrdersPurchase,
  type DistribuidoraPurchaseOrder,
} from "@/lib/api"
import type { PurchaseInvoiceStatusFilter } from "@/lib/purchase-invoice-status"
import { LiveBsaleSyncPanel } from "@/components/distribuidora/orders/LiveBsaleSyncPanel"
import { OrdersFilters, type SellerOption } from "@/components/distribuidora/orders/OrdersFilters"
import { OrdersTable } from "@/components/distribuidora/orders/OrdersTable"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
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
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [pageSelectedIds, setPageSelectedIds] = useState<Set<number>>(() => new Set())

  const deliverySearch = useMemo(() => {
    const parts = [...selectedDeliveryDays]
    if (deliveryExtraText.trim()) parts.push(deliveryExtraText.trim())
    return parts.length ? parts.join(",") : undefined
  }, [selectedDeliveryDays, deliveryExtraText])

  const municipalityCsv = useMemo(() => {
    if (selectedMunicipalityKeys.size === 0) return undefined
    return [...selectedMunicipalityKeys].join(",")
  }, [selectedMunicipalityKeys])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
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
        limit: 5000,
        offset: 0,
      })
      setItems(res.items)
      setTotal(res.total)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar órdenes")
      setItems([])
      setTotal(0)
    } finally {
      setLoading(false)
    }
  }, [
    dateFrom,
    dateTo,
    onlyNotInvoiced,
    invoiceStatusFilter,
    sellerUserId,
    deliverySearch,
    municipalityCsv,
  ])

  useEffect(() => {
    void load()
  }, [load])

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
          onClick={() => void load()}
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
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <LiveBsaleSyncPanel onSyncComplete={() => void load()} />

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

      <p className="text-sm text-muted-foreground">
        {loading ? "Cargando…" : `${items.length} de ${total} órdenes en el rango`}
      </p>

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
    </div>
  )
}
