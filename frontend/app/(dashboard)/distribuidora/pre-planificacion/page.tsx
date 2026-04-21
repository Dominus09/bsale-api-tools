"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Loader2 } from "lucide-react"

import {
  getDistribuidoraPlanificacionOrders,
  type DistribuidoraPlanificacionOrderRow,
} from "@/lib/api"
import {
  writePlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

const TRUCKS = ["Camión 1", "Camión 2", "Camión 3"] as const

const DELIVERY_OPTIONS: { value: string; label: string }[] = [
  { value: "all", label: "Todos" },
  { value: "lunes", label: "Lunes" },
  { value: "martes", label: "Martes" },
  { value: "miercoles", label: "Miércoles" },
  { value: "jueves", label: "Jueves" },
  { value: "viernes", label: "Viernes" },
  { value: "sabado", label: "Sábado" },
]

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

export default function PrePlanificacionDespachoPage() {
  const router = useRouter()
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [deliveryDay, setDeliveryDay] = useState("all")

  const [rows, setRows] = useState<DistribuidoraPlanificacionOrderRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [feedback, setFeedback] = useState<string | null>(null)

  const [selected, setSelected] = useState<Set<number>>(() => new Set())
  const [truckByDoc, setTruckByDoc] = useState<Record<number, string>>({})

  useEffect(() => {
    const ac = new AbortController()
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const res = await getDistribuidoraPlanificacionOrders({
          emission_date_from: dateFrom,
          emission_date_to: dateTo,
          delivery_day: deliveryDay === "all" ? undefined : deliveryDay,
          signal: ac.signal,
        })
        if (cancelled) return
        setRows(res.items)
        setSelected(new Set())
        setTruckByDoc({})
      } catch (e: unknown) {
        if (cancelled || (e instanceof Error && e.name === "AbortError")) return
        setError(e instanceof Error ? e.message : "Error al cargar")
        setRows([])
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
      ac.abort()
    }
  }, [dateFrom, dateTo, deliveryDay])

  useEffect(() => {
    setTruckByDoc((prev) => {
      const next = { ...prev }
      for (const r of rows) {
        if (next[r.document_id] === undefined) next[r.document_id] = TRUCKS[0]
      }
      for (const k of Object.keys(next)) {
        const id = Number(k)
        if (!rows.some((r) => r.document_id === id)) delete next[id]
      }
      return next
    })
  }, [rows])

  const toggle = useCallback((id: number, checked: boolean, canSelect: boolean) => {
    if (!canSelect) return
    setSelected((prev) => {
      const n = new Set(prev)
      if (checked) n.add(id)
      else n.delete(id)
      return n
    })
  }, [])

  const selectedWithGeo = useMemo(() => {
    const list: DistribuidoraPlanificacionOrderRow[] = []
    for (const r of rows) {
      if (!selected.has(r.document_id)) continue
      if (!r.has_georef || r.lat == null || r.lng == null) continue
      list.push(r)
    }
    return list
  }, [rows, selected])

  const onSubmit = useCallback(() => {
    setFeedback(null)
    if (selected.size === 0) {
      setFeedback("Seleccione al menos una orden.")
      return
    }
    const missingTruck: number[] = []
    const missingGeo: number[] = []
    const ordersOut: PlanificacionStoredOrder[] = []
    const byTruckOrder: Record<string, number> = {}

    for (const r of rows) {
      if (!selected.has(r.document_id)) continue
      if (!r.has_georef || r.lat == null || r.lng == null) {
        missingGeo.push(r.document_id)
        continue
      }
      const camion = truckByDoc[r.document_id]?.trim()
      if (!camion) {
        missingTruck.push(r.document_id)
        continue
      }
      const idx = (byTruckOrder[camion] = (byTruckOrder[camion] ?? 0) + 1)
      ordersOut.push({
        document_id: r.document_id,
        client_id: r.client_id ?? null,
        lat: Number(r.lat),
        lng: Number(r.lng),
        camion,
        oc: r.oc ?? null,
        nombre_fantasia: r.nombre_fantasia ?? null,
        total_amount: r.total_amount != null ? Number(r.total_amount) : null,
        stop_index: idx,
      })
    }

    if (missingGeo.length) {
      setFeedback("Hay filas seleccionadas sin georreferencia; desmárquelas o corrija datos de cliente.")
      return
    }
    if (missingTruck.length) {
      setFeedback("Cada fila seleccionada debe tener camión asignado.")
      return
    }

    writePlanificacionPayload({
      submittedAt: new Date().toISOString(),
      orders: ordersOut,
    })

    router.push("/distribuidora/planificacion")
  }, [rows, selected, truckByDoc, router])

  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-8 pb-16">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Pre‑planificación de despacho</h1>
          <p className="text-sm text-muted-foreground">
            Selección manual de boletas/facturas (tipos 1 y 6) con filtro por día en observaciones.
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link href="/distribuidora/planificacion">Ir a planificación (mapa)</Link>
        </Button>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {feedback ? (
        <Alert>
          <AlertTitle>No se puede continuar</AlertTitle>
          <AlertDescription>{feedback}</AlertDescription>
        </Alert>
      ) : null}

      <section className="grid gap-4 rounded-xl border border-border/60 bg-card/50 p-5 shadow-sm sm:grid-cols-2 lg:grid-cols-4">
        <div className="space-y-2">
          <Label>Fecha desde</Label>
          <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
        </div>
        <div className="space-y-2">
          <Label>Fecha hasta</Label>
          <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
        </div>
        <div className="space-y-2 sm:col-span-2 lg:col-span-2">
          <Label>Día de entrega (observaciones)</Label>
          <Select value={deliveryDay} onValueChange={setDeliveryDay}>
            <SelectTrigger className="w-full max-w-md">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {DELIVERY_OPTIONS.map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground">
            Coincidencia insensible a tildes sobre texto de observaciones y comentarios del documento.
          </p>
        </div>
      </section>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando órdenes…
        </div>
      ) : null}

      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground">
          Seleccionadas con georef:{" "}
          <strong className="text-foreground">{selectedWithGeo.length}</strong> / {selected.size}{" "}
          marcadas
        </p>
        <Button type="button" onClick={onSubmit} disabled={loading || selected.size === 0}>
          Enviar a planificación
        </Button>
      </div>

      <div className="overflow-x-auto rounded-xl border border-border/50">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="w-10" />
              <TableHead>OC</TableHead>
              <TableHead>Nombre fantasía</TableHead>
              <TableHead>Municipality</TableHead>
              <TableHead>Dirección</TableHead>
              <TableHead>Comuna</TableHead>
              <TableHead>Vendedor</TableHead>
              <TableHead className="text-right">Monto</TableHead>
              <TableHead>Georef</TableHead>
              <TableHead className="min-w-[9rem]">Camión</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 && !loading ? (
              <TableRow>
                <TableCell colSpan={10} className="py-10 text-center text-muted-foreground">
                  Sin resultados en el rango y filtro de día.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((r) => {
                const geo = Boolean(r.has_georef && r.lat != null && r.lng != null)
                return (
                  <TableRow key={r.document_id} className="text-sm">
                    <TableCell>
                      <Checkbox
                        checked={selected.has(r.document_id)}
                        disabled={!geo}
                        onCheckedChange={(c) =>
                          toggle(r.document_id, c === true, geo)
                        }
                        aria-label={`Seleccionar OC ${r.oc ?? r.document_id}`}
                      />
                    </TableCell>
                    <TableCell className="font-mono tabular-nums">{r.oc ?? "—"}</TableCell>
                    <TableCell className="max-w-[10rem] truncate">
                      {r.nombre_fantasia?.trim() || "—"}
                    </TableCell>
                    <TableCell className="max-w-[8rem] truncate">
                      {r.municipality?.trim() || "—"}
                    </TableCell>
                    <TableCell className="max-w-[12rem] truncate">
                      {r.direccion?.trim() || "—"}
                    </TableCell>
                    <TableCell className="max-w-[8rem] truncate">{r.comuna?.trim() || "—"}</TableCell>
                    <TableCell className="max-w-[8rem] truncate">
                      {r.seller_name?.trim() || "—"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatCLP(Number(r.total_amount ?? 0))}
                    </TableCell>
                    <TableCell>
                      {geo ? (
                        <Badge className="bg-emerald-600 hover:bg-emerald-600">Sí</Badge>
                      ) : (
                        <Badge variant="destructive">No</Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      <Select
                        value={truckByDoc[r.document_id] ?? TRUCKS[0]}
                        onValueChange={(v) =>
                          setTruckByDoc((prev) => ({ ...prev, [r.document_id]: v }))
                        }
                        disabled={!geo}
                      >
                        <SelectTrigger className="h-8 w-[8.5rem] text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {TRUCKS.map((t) => (
                            <SelectItem key={t} value={t}>
                              {t}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
