"use client"

/**
 * Clientes de un despacho — seguimiento (solo lectura).
 */

import { Fragment, useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { ChevronDown, ChevronRight, Eye, Loader2, RefreshCw } from "lucide-react"

import {
  getSeguimientoPlanClientes,
  getSeguimientoVisitDetail,
  type DeliveryVisitErpDetail,
  type DeliveryVisitErpMediaItem,
  type SeguimientoClientRow,
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

const RESULT_ES: Record<string, string> = {
  completed: "Completa",
  partial: "Parcial",
  not_delivered: "No entregada",
  pending: "Pendiente",
  in_progress: "En atención",
}

function MediaBlock({
  title,
  item,
}: {
  title: string
  item: DeliveryVisitErpMediaItem | null | undefined
}) {
  if (!item) {
    return (
      <div className="rounded border p-3 text-sm text-muted-foreground">
        {title}: Sin evidencia fotográfica
      </div>
    )
  }
  return (
    <div className="space-y-2 rounded border p-3">
      <div className="text-sm font-medium">{item.label_es || title}</div>
      <div className="text-xs text-muted-foreground">
        Estado: {item.validation_status || "—"}
        {item.has_gps != null ? ` · GPS: ${item.has_gps ? "sí" : "no"}` : ""}
        {item.captured_at ? ` · Captura: ${item.captured_at}` : ""}
      </div>
      {item.note_es ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">{item.note_es}</p>
      ) : null}
      {item.url ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={item.url}
          alt={item.label_es || title}
          className="max-h-[50vh] w-full rounded object-contain bg-muted/30"
        />
      ) : (
        <p className="text-sm text-muted-foreground">Sin evidencia fotográfica</p>
      )}
    </div>
  )
}

export default function SeguimientoPlanClientesPage() {
  const params = useParams()
  const planId = Number(params?.planId)

  const [status, setStatus] = useState("")
  const [clientInput, setClientInput] = useState("")
  const [clientQ, setClientQ] = useState("")
  const [plan, setPlan] = useState<{
    id: number
    planning_code?: string
    planning_date?: string
    route_name?: string | null
    driver_name?: string | null
    vehicle_label?: string | null
  } | null>(null)
  const [clients, setClients] = useState<SeguimientoClientRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detail, setDetail] = useState<DeliveryVisitErpDetail | null>(null)

  const load = useCallback(async () => {
    if (!Number.isFinite(planId) || planId <= 0) return
    setLoading(true)
    setError(null)
    try {
      const res = await getSeguimientoPlanClientes(planId, {
        status: status || undefined,
        client_q: clientQ || undefined,
      })
      setPlan(res.plan)
      setClients(res.clients || [])
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [planId, status, clientQ])

  useEffect(() => {
    void load()
  }, [load])

  async function openEvidence(visitId: number) {
    setDetailOpen(true)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)
    try {
      const res = await getSeguimientoVisitDetail(visitId)
      setDetail(res)
    } catch (e) {
      setDetailError(e instanceof Error ? e.message : "Error al cargar evidencia")
    } finally {
      setDetailLoading(false)
    }
  }

  return (
    <div className="space-y-4 p-4 sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Link
            href="/distribuidora/seguimiento-entregas"
            className="text-xs text-muted-foreground underline-offset-2 hover:underline"
          >
            ← Seguimiento de entregas
          </Link>
          <h1 className="mt-1 text-2xl font-semibold text-lq-ink">
            {plan?.planning_code || `Despacho ${planId}`}
          </h1>
          <p className="text-sm text-muted-foreground">
            {[plan?.driver_name, plan?.vehicle_label, plan?.route_name, plan?.planning_date]
              .filter(Boolean)
              .join(" · ") || "Clientes del despacho (consulta)"}
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          <span className="ml-2">Actualizar</span>
        </Button>
      </div>

      <div className="flex flex-wrap gap-3">
        <select
          className="flex h-10 rounded-md border border-input bg-background px-3 text-sm"
          value={status}
          onChange={(e) => setStatus(e.target.value)}
        >
          <option value="">Todos los estados</option>
          <option value="completed">Completas</option>
          <option value="partial">Parciales</option>
          <option value="not_delivered">No entregadas</option>
          <option value="pending">Pendientes</option>
        </select>
        <Input
          value={clientInput}
          onChange={(e) => setClientInput(e.target.value)}
          placeholder="Filtrar cliente"
          className="w-[200px]"
          onKeyDown={(e) => {
            if (e.key === "Enter") setClientQ(clientInput.trim())
          }}
        />
        <Button variant="secondary" size="sm" onClick={() => setClientQ(clientInput.trim())}>
          Filtrar
        </Button>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="text-xs whitespace-pre-wrap">{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-8" />
              <TableHead>#</TableHead>
              <TableHead>Cliente</TableHead>
              <TableHead>Docs</TableHead>
              <TableHead>Resultado</TableHead>
              <TableHead>Motivo</TableHead>
              <TableHead>Obs.</TableHead>
              <TableHead>Gestión</TableHead>
              <TableHead>Pago</TableHead>
              <TableHead>GPS</TableHead>
              <TableHead>Sync</TableHead>
              <TableHead>Evidencia</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {clients.length === 0 ? (
              <TableRow>
                <TableCell colSpan={12} className="text-muted-foreground">
                  {loading ? "Cargando…" : "Sin clientes"}
                </TableCell>
              </TableRow>
            ) : (
              clients.map((c) => {
                const open = !!expanded[c.customer_key]
                return (
                  <Fragment key={c.customer_key}>
                    <TableRow>
                      <TableCell>
                        <button
                          type="button"
                          className="text-muted-foreground"
                          onClick={() =>
                            setExpanded((prev) => ({
                              ...prev,
                              [c.customer_key]: !prev[c.customer_key],
                            }))
                          }
                          aria-label="Expandir documentos"
                        >
                          {open ? (
                            <ChevronDown className="h-4 w-4" />
                          ) : (
                            <ChevronRight className="h-4 w-4" />
                          )}
                        </button>
                      </TableCell>
                      <TableCell className="tabular-nums">{c.route_order ?? "—"}</TableCell>
                      <TableCell className="font-medium">{c.commercial_name || "—"}</TableCell>
                      <TableCell className="tabular-nums">{c.document_count ?? 0}</TableCell>
                      <TableCell>
                        {RESULT_ES[c.result_status || ""] || c.result_status || "—"}
                      </TableCell>
                      <TableCell className="max-w-[140px] truncate">
                        {c.result_reason_es || c.result_reason || "—"}
                      </TableCell>
                      <TableCell className="max-w-[160px] truncate">
                        {c.observation || "—"}
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-xs">
                        <div className="tabular-nums">{c.managed_at_local || "—"}</div>
                        <div className="text-muted-foreground">
                          {c.device_capture
                            ? "dispositivo"
                            : c.managed_at_source === "server"
                              ? "servidor"
                              : ""}
                          {c.synced_at_local ? ` · sync ${c.synced_at_local}` : ""}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs">{c.payment_declared || "—"}</TableCell>
                      <TableCell className="text-xs">
                        {c.requires_review
                          ? "pending_review"
                          : c.closure_review_status || "—"}
                      </TableCell>
                      <TableCell className="text-xs">
                        {c.sync_confirmed ? "Confirmado" : "—"}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span className="text-xs">{c.evidence_label_es || "—"}</span>
                          {c.visit_id ? (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 px-2"
                              onClick={() => void openEvidence(c.visit_id!)}
                            >
                              <Eye className="h-3.5 w-3.5" />
                            </Button>
                          ) : null}
                        </div>
                      </TableCell>
                    </TableRow>
                    {open ? (
                      <TableRow>
                        <TableCell colSpan={12} className="bg-muted/20 text-xs">
                          <div className="grid gap-3 sm:grid-cols-2">
                            <div>
                              <div className="mb-1 font-medium">Documentos</div>
                              {(c.documents || []).length === 0 ? (
                                <p className="text-muted-foreground">Sin documentos en visita</p>
                              ) : (
                                <ul className="list-inside list-disc">
                                  {(c.documents || []).map((d) => (
                                    <li key={d.id ?? `${d.document_type}-${d.document_number}`}>
                                      {d.document_type || "Doc"} {d.document_number ?? "—"} ·{" "}
                                      {d.result_status || "—"}
                                    </li>
                                  ))}
                                </ul>
                              )}
                            </div>
                            <div>
                              <div className="mb-1 font-medium">Productos</div>
                              {(c.products || []).length === 0 ? (
                                <p className="text-muted-foreground">Sin líneas de resultado</p>
                              ) : (
                                <ul className="list-inside list-disc">
                                  {(c.products || []).map((p) => (
                                    <li key={p.id ?? p.product_name}>
                                      {p.product_name || "Producto"}: {p.delivered_units ?? 0}/
                                      {p.planned_units ?? "—"}
                                      {p.undelivered_units
                                        ? ` (no ent. ${p.undelivered_units})`
                                        : ""}
                                    </li>
                                  ))}
                                </ul>
                              )}
                            </div>
                          </div>
                        </TableCell>
                      </TableRow>
                    ) : null}
                  </Fragment>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-h-[90vh] max-w-2xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Evidencia de visita</DialogTitle>
            <DialogDescription>
              URL firmada de corta duración. Bucket privado — sin enlaces públicos.
            </DialogDescription>
          </DialogHeader>
          {detailLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Cargando…
            </div>
          ) : null}
          {detailError ? (
            <p className="text-sm text-destructive whitespace-pre-wrap">{detailError}</p>
          ) : null}
          {detail ? (
            <div className="space-y-3">
              {(detail.incident_evidence || []).length === 0 ? (
                <p className="text-sm text-muted-foreground">Sin evidencia fotográfica</p>
              ) : (
                (detail.incident_evidence || []).map((item, idx) => (
                  <MediaBlock
                    key={item.id ?? idx}
                    title={`Evidencia ${idx + 1}`}
                    item={item}
                  />
                ))
              )}
              {detail.conformity?.signature_url ? (
                <MediaBlock
                  title="Firma histórica"
                  item={{
                    kind: "signature",
                    label_es: "Firma histórica (no requerida para cierre V1)",
                    url: detail.conformity.signature_url,
                    expires_in_sec: detail.conformity.expires_in_sec,
                  }}
                />
              ) : null}
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
