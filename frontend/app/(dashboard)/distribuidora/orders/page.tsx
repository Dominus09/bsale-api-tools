"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"

import { OrdersFilters, type MunicipalityOption, type SellerOption } from "@/components/distribuidora/orders/OrdersFilters"
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

export default function DistribuidoraOrdersPage() {
  const { addPlanningDocuments, clearPlanningDocuments, planningDocumentIds } =
    useDistribuidoraPlanning()

  const [date, setDate] = useState(() => localIsoDate())
  const [sellerUserId, setSellerUserId] = useState("")
  const [municipalityKey, setMunicipalityKey] = useState("")
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(false)

  const [rawItems, setRawItems] = useState<DistribuidoraPurchaseOrder[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [pageSelectedIds, setPageSelectedIds] = useState<Set<number>>(() => new Set())
  const [feedback, setFeedback] = useState<string | null>(null)

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
          emission_date_from: date,
          emission_date_to: date,
          only_not_invoiced: onlyNotInvoiced,
          user_id: Number.isFinite(uid as number) ? uid : undefined,
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
  }, [date, sellerUserId, onlyNotInvoiced])

  const sellerOptions = useMemo((): SellerOption[] => {
    const byUser = new Map<number, string>()
    for (const r of rawItems) {
      const id = r.user_id
      if (id == null || !Number.isFinite(Number(id))) continue
      const n = Number(id)
      const label =
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
    for (const r of rawItems) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      seen.set(k, label)
    }
    return Array.from(seen.entries())
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [rawItems])

  useEffect(() => {
    if (
      municipalityKey &&
      !municipalityOptions.some((o) => o.value === municipalityKey)
    ) {
      setMunicipalityKey("")
    }
  }, [municipalityKey, municipalityOptions])

  const displayItems = useMemo(() => {
    if (!municipalityKey) return rawItems
    return rawItems.filter((r) => communeKey(r) === municipalityKey)
  }, [rawItems, municipalityKey])

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
            Distribuidora · filtro por día · selección para planificación de rutas
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
            <Button type="button" variant="outline" size="sm" onClick={() => clearPlanningDocuments()}>
              Vaciar cola
            </Button>
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
            Hay {total} órdenes en el día y se muestran {rawItems.length} (límite
            5000). Ajuste filtros o aumente el límite en API si lo necesita.
          </AlertDescription>
        </Alert>
      ) : null}

      <OrdersFilters
        date={date}
        onDateChange={setDate}
        sellerOptions={sellerOptions}
        sellerUserId={sellerUserId}
        onSellerUserIdChange={setSellerUserId}
        municipalityOptions={municipalityOptions}
        municipalityKey={municipalityKey}
        onMunicipalityKeyChange={setMunicipalityKey}
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
            Mostrando {displayItems.length} de {rawItems.length} filas cargadas
            {total ? ` · Total servidor: ${total}` : ""}
          </p>
        </>
      ) : null}
    </div>
  )
}
