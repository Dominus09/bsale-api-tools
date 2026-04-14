"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"
import { useRouter } from "next/navigation"

import {
  getDistribuidoraOrdersPurchase,
  type DistribuidoraPurchaseOrder,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

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

function sellerLabel(row: DistribuidoraPurchaseOrder): string {
  return (
    row.seller_name?.trim() ||
    row.seller?.trim() ||
    "(Sin vendedor)"
  )
}

const WEEKDAY_TOKENS = [
  "Lunes",
  "Martes",
  "Miércoles",
  "Jueves",
  "Viernes",
  "Sábado",
  "Domingo",
] as const

function observacionesMatchesAnyDay(
  observaciones: string | null | undefined,
  days: Set<string>,
): boolean {
  if (days.size === 0) return true
  const obs = (observaciones ?? "").toLowerCase()
  if (!obs.trim()) return false
  for (const d of days) {
    if (obs.includes(d.toLowerCase())) return true
  }
  return false
}

type MuniOption = { value: string; label: string }

export default function DispatchAnalysisPage() {
  const router = useRouter()
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(false)
  const [selectedDays, setSelectedDays] = useState<Set<string>>(() => new Set())
  const [selectedMunicipalities, setSelectedMunicipalities] = useState<Set<string>>(
    () => new Set(),
  )

  const [items, setItems] = useState<DistribuidoraPurchaseOrder[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraOrdersPurchase({
        emission_date_from: dateFrom,
        emission_date_to: dateTo,
        only_not_invoiced: onlyNotInvoiced,
        limit: 5000,
        offset: 0,
      })
      setItems(res.items)
      setTotal(res.total)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar datos")
      setItems([])
      setTotal(0)
    } finally {
      setLoading(false)
    }
  }, [dateFrom, dateTo, onlyNotInvoiced])

  useEffect(() => {
    void load()
  }, [load])

  const municipalityOptions = useMemo((): MuniOption[] => {
    const seen = new Map<string, string>()
    for (const r of items) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      seen.set(k, label)
    }
    return Array.from(seen.entries())
      .map(([value, label]) => ({ value, label }))
      .sort((a, b) => a.label.localeCompare(b.label, "es"))
  }, [items])

  const filtered = useMemo(() => {
    return items.filter((r) => {
      if (!observacionesMatchesAnyDay(r.observaciones, selectedDays)) return false
      if (selectedMunicipalities.size === 0) return true
      return selectedMunicipalities.has(communeKey(r))
    })
  }, [items, selectedDays, selectedMunicipalities])

  const summaryByMunicipality = useMemo(() => {
    const m = new Map<
      string,
      { label: string; clients: Set<number>; amount: number }
    >()
    for (const r of filtered) {
      const k = communeKey(r)
      const label = k === "__NONE__" ? "(Sin comuna / ciudad)" : k
      if (!m.has(k)) m.set(k, { label, clients: new Set(), amount: 0 })
      const row = m.get(k)!
      const cid = r.client_id
      if (cid != null && Number.isFinite(Number(cid))) row.clients.add(Number(cid))
      row.amount += Number(r.total_amount ?? 0)
    }
    return Array.from(m.entries())
      .map(([key, v]) => ({
        key,
        label: v.label,
        clientCount: v.clients.size,
        totalAmount: v.amount,
      }))
      .sort((a, b) => b.totalAmount - a.totalAmount)
  }, [filtered])

  const summaryBySeller = useMemo(() => {
    const m = new Map<string, { clients: Set<number>; amount: number }>()
    for (const r of filtered) {
      const k = sellerLabel(r)
      if (!m.has(k)) m.set(k, { clients: new Set(), amount: 0 })
      const row = m.get(k)!
      const cid = r.client_id
      if (cid != null && Number.isFinite(Number(cid))) row.clients.add(Number(cid))
      row.amount += Number(r.total_amount ?? 0)
    }
    return Array.from(m.entries())
      .map(([seller, v]) => ({
        seller,
        clientCount: v.clients.size,
        totalAmount: v.amount,
      }))
      .sort((a, b) => b.totalAmount - a.totalAmount)
  }, [filtered])

  const toggleDay = (d: string) => {
    setSelectedDays((prev) => {
      const n = new Set(prev)
      if (n.has(d)) n.delete(d)
      else n.add(d)
      return n
    })
  }

  const toggleMunicipality = (value: string) => {
    setSelectedMunicipalities((prev) => {
      const n = new Set(prev)
      if (n.has(value)) n.delete(value)
      else n.add(value)
      return n
    })
  }

  const clearMunicipalityFilters = () => setSelectedMunicipalities(new Set())

  const goToOrders = () => {
    const qs = new URLSearchParams()
    qs.set("emission_date_from", dateFrom)
    qs.set("emission_date_to", dateTo)
    if (onlyNotInvoiced) qs.set("only_not_invoiced", "true")
    if (selectedDays.size > 0) {
      qs.set("delivery_search", Array.from(selectedDays).join(","))
    }
    if (selectedMunicipalities.size > 0) {
      qs.set("municipality", Array.from(selectedMunicipalities).join(","))
    }
    router.push(`/distribuidora/orders?${qs.toString()}`)
  }

  const truncated = total > items.length

  return (
    <div className="mx-auto flex max-w-[1200px] flex-col gap-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">
          Análisis previo a despacho
        </h1>
        <p className="text-sm text-muted-foreground">
          Resumen por ciudad y vendedor según observaciones (día de entrega), antes de
          planificar camiones.
        </p>
      </div>

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
            Hay {total} órdenes en el rango y se cargaron {items.length} (límite 5000).
            Los totales pueden quedar incompletos.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-col gap-4 rounded-lg border bg-card p-4 shadow-sm">
        <h2 className="text-sm font-semibold">Filtros</h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-2">
            <Label htmlFor="da-from">Emisión desde</Label>
            <Input
              id="da-from"
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              disabled={loading}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="da-to">Emisión hasta</Label>
            <Input
              id="da-to"
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              disabled={loading}
            />
          </div>
          <div className="flex items-end pb-1 lg:col-span-2">
            <label className="flex cursor-pointer items-center gap-2 text-sm">
              <Checkbox
                checked={onlyNotInvoiced}
                onCheckedChange={(c) => setOnlyNotInvoiced(c === true)}
                disabled={loading}
              />
              Solo no facturadas
            </label>
          </div>
        </div>

        <div className="space-y-2">
          <Label>Día en observaciones (varios = OR)</Label>
          <div className="flex flex-wrap gap-2">
            {WEEKDAY_TOKENS.map((d) => (
              <Button
                key={d}
                type="button"
                size="sm"
                variant={selectedDays.has(d) ? "default" : "outline"}
                onClick={() => toggleDay(d)}
              >
                {d}
              </Button>
            ))}
          </div>
          <p className="text-xs text-muted-foreground">
            Si no marca ningún día, se incluyen todas las órdenes del rango en los
            resúmenes. Al elegir días, se filtra por texto en observaciones (sin
            distinguir mayúsculas).
          </p>
        </div>

        <div className="space-y-2">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label>Ciudad / comuna (opcional, varias)</Label>
            {selectedMunicipalities.size > 0 ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={clearMunicipalityFilters}
              >
                Limpiar ciudades
              </Button>
            ) : null}
          </div>
          <div className="max-h-40 overflow-y-auto rounded-md border p-2 text-sm">
            {municipalityOptions.length === 0 ? (
              <span className="text-muted-foreground">Sin datos en el rango.</span>
            ) : (
              <ul className="space-y-2">
                {municipalityOptions.map((o) => (
                  <li key={o.value} className="flex items-center gap-2">
                    <Checkbox
                      id={`muni-${o.value}`}
                      checked={selectedMunicipalities.has(o.value)}
                      onCheckedChange={() => toggleMunicipality(o.value)}
                    />
                    <label htmlFor={`muni-${o.value}`} className="cursor-pointer">
                      {o.label}
                    </label>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="secondary" onClick={() => void load()} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Cargando…
              </>
            ) : (
              "Actualizar datos"
            )}
          </Button>
          <Button type="button" onClick={goToOrders}>
            Ver órdenes filtradas
          </Button>
        </div>
      </div>

      <p className="text-sm text-muted-foreground">
        Órdenes consideradas en resúmenes:{" "}
        <strong className="text-foreground">{filtered.length}</strong>
        {items.length ? ` de ${items.length} cargadas` : ""}
      </p>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="space-y-2">
          <h2 className="text-lg font-semibold">Resumen por ciudad</h2>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Ciudad / comuna</TableHead>
                  <TableHead className="text-right">Clientes</TableHead>
                  <TableHead className="text-right">Monto total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {summaryByMunicipality.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3} className="text-muted-foreground">
                      Sin filas con los filtros actuales.
                    </TableCell>
                  </TableRow>
                ) : (
                  summaryByMunicipality.map((r) => (
                    <TableRow key={r.key}>
                      <TableCell>{r.label}</TableCell>
                      <TableCell className="text-right">{r.clientCount}</TableCell>
                      <TableCell className="text-right">
                        {r.totalAmount.toLocaleString("es-CL", {
                          style: "currency",
                          currency: "CLP",
                          maximumFractionDigits: 0,
                        })}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-semibold">Resumen por vendedor</h2>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Vendedor</TableHead>
                  <TableHead className="text-right">Clientes</TableHead>
                  <TableHead className="text-right">Monto total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {summaryBySeller.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3} className="text-muted-foreground">
                      Sin filas con los filtros actuales.
                    </TableCell>
                  </TableRow>
                ) : (
                  summaryBySeller.map((r) => (
                    <TableRow key={r.seller}>
                      <TableCell>{r.seller}</TableCell>
                      <TableCell className="text-right">{r.clientCount}</TableCell>
                      <TableCell className="text-right">
                        {r.totalAmount.toLocaleString("es-CL", {
                          style: "currency",
                          currency: "CLP",
                          maximumFractionDigits: 0,
                        })}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </section>
      </div>
    </div>
  )
}
