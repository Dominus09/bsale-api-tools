"use client"

/**
 * Seguimiento de entregas — consulta gerencial (solo lectura).
 * No modifica visitas, stock, Bsale ni cuadratura.
 */

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { Loader2, RefreshCw } from "lucide-react"

import {
  getSeguimientoEntregasDashboard,
  type SeguimientoDashboard,
  type SeguimientoDriverRow,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

const POLL_MS = 45_000

function todaySantiagoInput(): string {
  try {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Santiago",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date())
  } catch {
    return new Date().toISOString().slice(0, 10)
  }
}

const STATUS_OPTS = [
  { value: "", label: "Todos" },
  { value: "completed", label: "Completas" },
  { value: "partial", label: "Parciales" },
  { value: "not_delivered", label: "No entregadas" },
  { value: "pending", label: "Pendientes" },
]

function Kpi({ label, value }: { label: string; value: string | number | null | undefined }) {
  return (
    <div className="min-w-[140px] flex-1 border-b border-lq-border/60 py-3 pr-4">
      <div className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      <div className="mt-1 text-2xl font-semibold tabular-nums text-lq-ink">{value ?? "—"}</div>
    </div>
  )
}

export default function SeguimientoEntregasPage() {
  const [date, setDate] = useState(todaySantiagoInput)
  const [status, setStatus] = useState("")
  const [clientQ, setClientQ] = useState("")
  const [clientInput, setClientInput] = useState("")
  const [driverFilter, setDriverFilter] = useState<string>("")
  const [data, setData] = useState<SeguimientoDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getSeguimientoEntregasDashboard({
        date,
        status: status || undefined,
        client_q: clientQ || undefined,
        driver_user_id: driverFilter ? Number(driverFilter) : undefined,
      })
      setData(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [date, status, clientQ, driverFilter])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    const id = window.setInterval(() => {
      void load()
    }, POLL_MS)
    return () => window.clearInterval(id)
  }, [load])

  const drivers = useMemo(() => {
    const map = new Map<string, string>()
    for (const row of data?.by_driver || []) {
      if (row.driver_user_id != null) {
        map.set(String(row.driver_user_id), row.driver_name || `Chofer ${row.driver_user_id}`)
      }
    }
    return Array.from(map.entries())
  }, [data])

  const k = data?.kpis

  return (
    <div className="space-y-6 p-4 sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Distribuidora · Consulta
          </p>
          <h1 className="text-[28px] font-semibold leading-8 tracking-[-0.02em] text-lq-ink sm:text-[32px]">
            Seguimiento de entregas
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            Solo operaciones confirmadas en servidor. Zona horaria America/Santiago.
            Actualización automática cada 45 s.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          <span className="ml-2">Actualizar</span>
        </Button>
      </div>

      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1 text-sm">
          <span className="text-muted-foreground">Fecha</span>
          <Input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="w-[160px]"
          />
        </label>
        <label className="space-y-1 text-sm">
          <span className="text-muted-foreground">Estado</span>
          <select
            className="flex h-10 w-[160px] rounded-md border border-input bg-background px-3 text-sm"
            value={status}
            onChange={(e) => setStatus(e.target.value)}
          >
            {STATUS_OPTS.map((o) => (
              <option key={o.value || "all"} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label className="space-y-1 text-sm">
          <span className="text-muted-foreground">Chofer</span>
          <select
            className="flex h-10 min-w-[180px] rounded-md border border-input bg-background px-3 text-sm"
            value={driverFilter}
            onChange={(e) => setDriverFilter(e.target.value)}
          >
            <option value="">Todos</option>
            {drivers.map(([id, name]) => (
              <option key={id} value={id}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label className="space-y-1 text-sm">
          <span className="text-muted-foreground">Cliente</span>
          <div className="flex gap-2">
            <Input
              value={clientInput}
              onChange={(e) => setClientInput(e.target.value)}
              placeholder="Nombre comercial"
              className="w-[200px]"
              onKeyDown={(e) => {
                if (e.key === "Enter") setClientQ(clientInput.trim())
              }}
            />
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => setClientQ(clientInput.trim())}
            >
              Filtrar
            </Button>
          </div>
        </label>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>No se pudo cargar</AlertTitle>
          <AlertDescription className="whitespace-pre-wrap text-xs">{error}</AlertDescription>
        </Alert>
      ) : null}

      {data?.tables_ready === false ? (
        <Alert>
          <AlertTitle>Esquema pendiente</AlertTitle>
          <AlertDescription>{data.message_es || "Migración 056 pendiente"}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap gap-x-6 border-y border-lq-border/40 py-1">
        <Kpi label="Planificados" value={k?.planned_clients} />
        <Kpi label="Gestionados" value={k?.managed_clients} />
        <Kpi label="Completas" value={k?.completed} />
        <Kpi label="Parciales" value={k?.partial} />
        <Kpi label="Local cerrado" value={k?.closed_store} />
        <Kpi label="Otros no entregados" value={k?.other_not_delivered} />
        <Kpi label="Pendientes" value={k?.pending_clients} />
        <Kpi label="GPS por revisar" value={k?.pending_gps_review} />
        <Kpi label="Primera entrega" value={k?.first_delivery_at_local} />
        <Kpi label="Última entrega" value={k?.last_delivery_at_local} />
        <Kpi label="Última act. panel" value={data?.panel_updated_at_local} />
      </div>

      <div>
        <h2 className="mb-2 text-base font-semibold text-lq-ink">Por chofer / despacho</h2>
        <p className="mb-3 text-xs text-muted-foreground">
          Las horas de gestión priorizan la captura del dispositivo. La sync al servidor se detalla
          en cada cliente.
        </p>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Chofer</TableHead>
                <TableHead>Vehículo</TableHead>
                <TableHead>Despacho</TableHead>
                <TableHead>Ruta</TableHead>
                <TableHead className="text-right">Tot</TableHead>
                <TableHead className="text-right">Ent</TableHead>
                <TableHead className="text-right">Parc</TableHead>
                <TableHead className="text-right">No ent</TableHead>
                <TableHead className="text-right">Pend</TableHead>
                <TableHead className="text-right">%</TableHead>
                <TableHead>1ª</TableHead>
                <TableHead>Últ ent</TableHead>
                <TableHead>Últ gest</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.by_driver || []).length === 0 ? (
                <TableRow>
                  <TableCell colSpan={13} className="text-muted-foreground">
                    {loading ? "Cargando…" : "Sin despachos para la fecha"}
                  </TableCell>
                </TableRow>
              ) : (
                (data?.by_driver || []).map((row: SeguimientoDriverRow) => (
                  <TableRow key={row.dispatch_plan_id}>
                    <TableCell className="font-medium">{row.driver_name || "—"}</TableCell>
                    <TableCell>{row.vehicle_label || row.truck_plate || "—"}</TableCell>
                    <TableCell>
                      <Link
                        href={`/distribuidora/seguimiento-entregas/planes/${row.dispatch_plan_id}`}
                        className="text-lq-ink underline-offset-2 hover:underline"
                      >
                        {row.planning_code || row.dispatch_plan_id}
                      </Link>
                    </TableCell>
                    <TableCell className="max-w-[160px] truncate">{row.route_name || "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{row.clients_total ?? 0}</TableCell>
                    <TableCell className="text-right tabular-nums">{row.delivered ?? 0}</TableCell>
                    <TableCell className="text-right tabular-nums">{row.partial ?? 0}</TableCell>
                    <TableCell className="text-right tabular-nums">{row.not_delivered ?? 0}</TableCell>
                    <TableCell className="text-right tabular-nums">{row.pending ?? 0}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      {row.progress_percent ?? 0}%
                    </TableCell>
                    <TableCell className="tabular-nums">{row.first_delivery_at_local || "—"}</TableCell>
                    <TableCell className="tabular-nums">{row.last_delivery_at_local || "—"}</TableCell>
                    <TableCell className="tabular-nums">{row.last_management_at_local || "—"}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </div>

      {data?.note_es ? (
        <p className="text-xs text-muted-foreground">{data.note_es}</p>
      ) : null}
      {data?.evidence_policy?.note_es ? (
        <p className="text-xs text-muted-foreground">{data.evidence_policy.note_es}</p>
      ) : null}
    </div>
  )
}
