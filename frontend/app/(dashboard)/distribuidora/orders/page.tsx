"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"
import Link from "next/link"
import { useSearchParams } from "next/navigation"

import {
  OrdersFilters,
  WEEKDAY_DELIVERY_OPTIONS,
  type MunicipalityOption,
  type SellerOption,
} from "@/components/distribuidora/orders/OrdersFilters"
import { OrdersSummary } from "@/components/distribuidora/orders/OrdersSummary"
import { OrdersTable } from "@/components/distribuidora/orders/OrdersTable"
import { useDistribuidoraPlanning } from "@/context/distribuidora-planning-selection"
import {
  getDistribuidoraOrdersPurchase,
  type DistribuidoraPurchaseOrder,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function communeKey(row: DistribuidoraPurchaseOrder): string {
  const t = row.municipality?.trim() || row.city?.trim()
  return t || "__NONE__"
}

function parseDeliveryFromUrl(del: string | undefined): {
  days: Set<string>
  extra: string
} {
  if (!del?.trim()) return { days: new Set(), extra: "" }
  const days = new Set<string>()
  const extras: string[] = []
  for (const part of del.split(",").map((s) => s.trim()).filter(Boolean)) {
    let matched = false
    for (const d of WEEKDAY_DELIVERY_OPTIONS) {
      if (d.toLowerCase() === part.toLowerCase()) {
        days.add(d)
        matched = true
        break
      }
    }
    if (!matched) extras.push(part)
  }
  return { days, extra: extras.join(", ") }
}

export default function DistribuidoraOrdersPage() {
  const searchParams = useSearchParams()
  const queryString = searchParams.toString()
  const { addPlanningDocuments, clearPlanningDocuments, planningDocumentIds } =
    useDistribuidoraPlanning()

  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [selectedDeliveryDays, setSelectedDeliveryDays] = useState<Set<string>>(
    () => new Set(),
  )
  const [deliveryExtraText, setDeliveryExtraText] = useState("")
  const [sellerUserId, setSellerUserId] = useState("")
  const [selectedMunicipalities, setSelectedMunicipalities] = useState<Set<string>>(
    () => new Set(),
  )
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(false)

  const [rawItems, setRawItems] = useState<DistribuidoraPurchaseOrder[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [pageSelectedIds, setPageSelectedIds] = useState<Set<number>>(() => new Set())
  const [feedback, setFeedback] = useState<string | null>(null)

  useEffect(() => {
    const sp = new URLSearchParams(queryString)
    const d0 =
      sp.get("emission_date_from")?.trim() || sp.get("date")?.trim()
    const d1 = sp.get("emission_date_to")?.trim() || sp.get("date_to")?.trim()
    if (d0) setDateFrom(d0)
    if (d1) setDateTo(d1)
    else if (d0) setDateTo(d0)
    const del =
      sp.get("delivery_search")?.trim() || sp.get("observaciones")?.trim()
    if (sp.has("delivery_search") || sp.has("observaciones")) {
      const { days, extra } = parseDeliveryFromUrl(del ?? undefined)
      setSelectedDeliveryDays(days)
      setDeliveryExtraText(extra)
    }
    const muni = sp.get("municipality")?.trim() || sp.get("ciudad")?.trim()
    if (sp.has("municipality") || sp.has("ciudad")) {
      if (muni) {
        setSelectedMunicipalities(
          new Set(muni.split(",").map((s) => s.trim()).filter(Boolean)),
        )
      } else {
        setSelectedMunicipalities(new Set())
      }
    }
    if (sp.get("only_not_invoiced") === "true") setOnlyNotInvoiced(true)
    else if (sp.has("only_not_invoiced")) setOnlyNotInvoiced(false)
    if (sp.has("user_id")) {
      const u = sp.get("user_id")?.trim()
      if (u && Number.isFinite(Number.parseInt(u, 10))) setSellerUserId(u)
      else setSellerUserId("")
    }
  }, [queryString])

  const deliverySearchParam = useMemo(() => {
    const parts: string[] = []
    const days = Array.from(selectedDeliveryDays).sort((a, b) =>
      a.localeCompare(b, "es"),
    )
    parts.push(...days)
    const extra = deliveryExtraText.trim()
    if (extra) parts.push(extra)
    if (parts.length === 0) return undefined
    return parts.join(",")
  }, [selectedDeliveryDays, deliveryExtraText])

  const municipalityApiParam = useMemo(() => {
    if (selectedMunicipalities.size === 0) return undefined
    return Array.from(selectedMunicipalities).join(",")
  }, [selectedMunicipalities])

  const toggleDeliveryDay = useCallback((day: string) => {
    setSelectedDeliveryDays((prev) => {
      const n = new Set(prev)
      if (n.has(day)) n.delete(day)
      else n.add(day)
      return n
    })
  }, [])

  const toggleMunicipality = useCallback((value: string) => {
    setSelectedMunicipalities((prev) => {
      const n = new Set(prev)
      if (n.has(value)) n.delete(value)
      else n.add(value)
      return n
    })
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const uid =
          sellerUserId === "" ? undefined : parseInt(sellerUserId, 10)
        const res = await getDistribuidoraOrdersPurchase({
          emission_date_from: dateFrom,
          emission_date_to: dateTo,
          only_not_invoiced: onlyNotInvoiced,
          user_id: Number.isFinite(uid as number) ? uid : undefined,
          delivery_search: deliverySearchParam,
          municipality: municipalityApiParam,
          limit: 5000,
          offset: 0,
          signal: ac.signal,
        })
        if (cancelled) return
        setRawItems(res.items)
        setTotal(res.total)
      } catch (e: unknown) {
        if (cancelled || (e instanceof Error && e.name === "AbortError")) return
        setError(e instanceof Error ? e.message : "Error al cargar órdenes")
        setRawItems([])
        setTotal(0)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
      ac.abort()
    }
  }, [
    dateFrom,
    dateTo,
    sellerUserId,
    onlyNotInvoiced,
    deliverySearchParam,
    municipalityApiParam,
  ])

  const sellerOptions = useMemo((): SellerOption[] => {
    const byUser = new Map<number, string>()
    for (const r of rawItems) {
      const id = r.user_id
      if (id == null || !Number.isFinite(Number(id))) continue
      const n = Number(id)
      const label =
        r.seller_name?.trim() ||
        r.seller?.trim() ||
        (Number.isFinite(n) ? `Usuario ${n}` : "Usuario")
      if (!byUser.has(n)) byUser.set(n, label)
    }
    return Array.from(byUser.entries())
      .map(([user_id, label]) => ({ user_id, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [rawItems])

  const municipalityOptions = useMemo((): MunicipalityOption[] => {
    const seen = new Map<string, string>()
    for (const k of selectedMunicipalities) {
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      seen.set(k, label)
    }
    for (const r of rawItems) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      seen.set(k, label)
    }
    return Array.from(seen.entries())
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [rawItems, selectedMunicipalities])

  const displayItems = rawItems

  const truncated = total > rawItems.length

  const toggle = useCallback((documentId: number, checked: boolean) => {
    setPageSelectedIds((prev) => {
      const n = new Set(prev)
      if (checked) n.add(documentId)
      else n.delete(documentId)
      return n
    })
  }, [])

  const toggleAll = useCallback(
    (checked: boolean) => {
      setPageSelectedIds(() => {
        if (!checked) return new Set()
        return new Set(displayItems.map((r) => r.document_id))
      })
    },
    [displayItems],
  )

  const onAddToPlanning = useCallback(() => {
    const ids = Array.from(pageSelectedIds)
    if (ids.length === 0) return
    const prevQueue = planningDocumentIds.size
    addPlanningDocuments(ids)
    setPageSelectedIds(new Set())
    setFeedback(
      `Se añadieron ${ids.length} orden(es) a la cola de planificación (total en cola: ${prevQueue + ids.length}).`,
    )
    window.setTimeout(() => setFeedback(null), 5000)
  }, [pageSelectedIds, addPlanningDocuments, planningDocumentIds.size])

  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-6">
      <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Órdenes de compra
          </h1>
          <p className="text-sm text-muted-foreground">
            Distribuidora · filtros combinables (fechas, comuna, vendedor, observaciones)
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          <span>
            En cola planificación:{" "}
            <strong className="text-foreground">
              {planningDocumentIds.size}
            </strong>
          </span>
          {planningDocumentIds.size > 0 ? (
            <>
              <Button asChild size="sm">
                <Link href="/distribuidora/planning">Planificar camiones</Link>
              </Button>
              <Button type="button" variant="outline" size="sm" onClick={() => clearPlanningDocuments()}>
                Vaciar cola
              </Button>
            </>
          ) : null}
        </div>
      </div>

      {feedback ? (
        <Alert>
          <AlertTitle>Listo</AlertTitle>
          <AlertDescription>{feedback}</AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {truncated ? (
        <Alert>
          <AlertTitle>Aviso</AlertTitle>
          <AlertDescription>
            Hay {total} órdenes en el rango y se muestran {rawItems.length} (límite
            5000). Ajuste filtros o aumente el límite en API si lo necesita.
          </AlertDescription>
        </Alert>
      ) : null}

      <OrdersFilters
        dateFrom={dateFrom}
        onDateFromChange={setDateFrom}
        dateTo={dateTo}
        onDateToChange={setDateTo}
        selectedDeliveryDays={selectedDeliveryDays}
        onToggleDeliveryDay={toggleDeliveryDay}
        onClearDeliveryDays={() => setSelectedDeliveryDays(new Set())}
        deliveryExtraText={deliveryExtraText}
        onDeliveryExtraTextChange={setDeliveryExtraText}
        sellerOptions={sellerOptions}
        sellerUserId={sellerUserId}
        onSellerUserIdChange={setSellerUserId}
        municipalityOptions={municipalityOptions}
        selectedMunicipalityKeys={selectedMunicipalities}
        onToggleMunicipality={toggleMunicipality}
        onClearMunicipalities={() => setSelectedMunicipalities(new Set())}
        onlyNotInvoiced={onlyNotInvoiced}
        onOnlyNotInvoicedChange={setOnlyNotInvoiced}
        loading={loading}
      />

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Cargando órdenes…
        </div>
      ) : null}

      {!loading ? (
        <>
          <OrdersSummary items={displayItems} />
          <OrdersTable
            items={displayItems}
            pageSelectedIds={pageSelectedIds}
            onToggle={toggle}
            onToggleAll={toggleAll}
            onAddToPlanning={onAddToPlanning}
            planningBasketCount={planningDocumentIds.size}
            loading={loading}
          />
          <p className="text-xs text-muted-foreground">
            Mostrando {displayItems.length} filas
            {total ? ` · Total servidor: ${total}` : ""}
          </p>
        </>
      ) : null}
    </div>
  )
}
