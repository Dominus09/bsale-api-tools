"use client"

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react"
import Link from "next/link"
import { Loader2, Lock, Plus, Save, Trash2 } from "lucide-react"

import {
  closeDispatchPlanCuadratura,
  getDispatchPlanCuadratura,
  putDispatchPlanCuadratura,
  type DispatchPlanCuadraturaResponse,
} from "@/lib/api"
import {
  MEDIO_PAGO_LABELS,
  MEDIOS_PAGO,
  computeCuadraturaV2Result,
  diffStatusClass,
  emptyCreditNoteV2Row,
  emptyNotLoadedV2Row,
  enrichNotLoadedRows,
  observacionRequired,
  operationalStatusBadge,
  type CuadraturaCreditNoteV2Row,
  type CuadraturaDocumentRow,
  type CuadraturaNotLoadedV2Row,
  type CuadraturaProductCatalogRow,
} from "@/lib/dispatch-plan-cuadratura"
import { formatClp } from "@/lib/ors-map-ui"
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
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"

type DispatchPlanCuadraturaPanelProps = {
  planId: number
  onMessage: (msg: string) => void
  showPlanLink?: boolean
}

function parseClpInput(raw: string): number {
  const n = Number(raw.replace(/\s/g, "").replace(/\./g, ""))
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : 0
}

function ReadOnlyAmount({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-border/60 bg-muted/20 p-3">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p className="font-mono text-sm font-semibold">{formatClp(value)}</p>
    </div>
  )
}

export function DispatchPlanCuadraturaPanel({
  planId,
  onMessage,
  showPlanLink = false,
}: DispatchPlanCuadraturaPanelProps) {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [closing, setClosing] = useState(false)
  const [data, setData] = useState<DispatchPlanCuadraturaResponse | null>(null)
  const [documents, setDocuments] = useState<CuadraturaDocumentRow[]>([])
  const [creditNotes, setCreditNotes] = useState<CuadraturaCreditNoteV2Row[]>([])
  const [notLoaded, setNotLoaded] = useState<CuadraturaNotLoadedV2Row[]>([])
  const [catalog, setCatalog] = useState<CuadraturaProductCatalogRow[]>([])
  const [observacion, setObservacion] = useState("")

  const isClosed = Boolean(data?.closed_at)
  const ventaPicking = data?.ventas.venta_picking_clp ?? 0

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await getDispatchPlanCuadratura(planId)
      setData(res)
      setDocuments((res.documents ?? []) as CuadraturaDocumentRow[])
      setCreditNotes((res.credit_notes_v2 ?? []) as CuadraturaCreditNoteV2Row[])
      const cat = (res.product_catalog ?? []) as CuadraturaProductCatalogRow[]
      setCatalog(cat)
      setNotLoaded(enrichNotLoadedRows((res.not_loaded_v2 ?? []) as CuadraturaNotLoadedV2Row[], cat))
      setObservacion(res.observacion || "")
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar cuadratura")
    } finally {
      setLoading(false)
    }
  }, [planId, onMessage])

  useEffect(() => {
    void load()
  }, [load])

  const resultado = useMemo(
    () =>
      computeCuadraturaV2Result({
        venta_picking_clp: ventaPicking,
        documents,
        credit_notes_v2: creditNotes,
        not_loaded_v2: notLoaded,
      }),
    [ventaPicking, documents, creditNotes, notLoaded],
  )

  const obsRequired = observacionRequired(resultado.diferencia_clp)
  const statusBadge = operationalStatusBadge(data?.operational_status || "pending")

  const updateNotLoaded = (rows: CuadraturaNotLoadedV2Row[]) => {
    setNotLoaded(enrichNotLoadedRows(rows, catalog))
  }

  const save = async () => {
    if (obsRequired && !observacion.trim()) {
      onMessage("Debe ingresar una observación cuando la diferencia es distinta de cero.")
      return
    }
    setSaving(true)
    try {
      await putDispatchPlanCuadratura(planId, {
        schema_version: 2,
        observacion: observacion.trim() || null,
        documents,
        credit_notes_v2: creditNotes,
        not_loaded_v2: notLoaded,
      })
      onMessage("Cuadratura guardada.")
      await load()
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al guardar cuadratura")
    } finally {
      setSaving(false)
    }
  }

  const closeCuadratura = async () => {
    if (obsRequired && !observacion.trim()) {
      onMessage("No se puede cerrar sin observación cuando hay diferencia.")
      return
    }
    setClosing(true)
    try {
      await putDispatchPlanCuadratura(planId, {
        schema_version: 2,
        observacion: observacion.trim() || null,
        documents,
        credit_notes_v2: creditNotes,
        not_loaded_v2: notLoaded,
      })
      await closeDispatchPlanCuadratura(planId, { observacion: observacion.trim() || null })
      onMessage("Cuadratura cerrada.")
      await load()
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cerrar cuadratura")
    } finally {
      setClosing(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        Cargando cuadratura…
      </div>
    )
  }

  if (!data?.picking_ready) {
    return (
      <p className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
        Se requiere picking generado para cuadratura documental. Genere picking en la pestaña
        correspondiente del plan.
      </p>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge className={cn("font-normal", statusBadge.className)}>
              {statusBadge.emoji} {statusBadge.label}
            </Badge>
            {data.picking_version != null ? (
              <span className="text-xs text-muted-foreground">
                Picking v{data.picking_version} congelado
              </span>
            ) : null}
            {isClosed ? (
              <Badge variant="outline" className="gap-1 font-normal">
                <Lock className="size-3" />
                Cerrada
              </Badge>
            ) : null}
          </div>
          {showPlanLink ? (
            <Link
              href={`/distribuidora/planificaciones/${planId}`}
              className="text-xs text-primary hover:underline"
            >
              Ver planificación completa
            </Link>
          ) : null}
        </div>
      </div>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Ventas (snapshot congelado)</h3>
        <div className="grid gap-2 sm:grid-cols-3">
          <ReadOnlyAmount label="Venta OC" value={data.ventas.venta_oc_clp} />
          <ReadOnlyAmount label="Venta facturada" value={data.ventas.venta_facturada_clp} />
          <ReadOnlyAmount label="Venta picking" value={ventaPicking} />
        </div>
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Documentos despachados</h3>
        <p className="text-xs text-muted-foreground">
          Medio de pago por documento — fuente: snapshot picking cliente.
        </p>
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Documento</TableHead>
                <TableHead>Cliente</TableHead>
                <TableHead className="text-right">Monto</TableHead>
                <TableHead>Medio de pago</TableHead>
                <TableHead>Observación</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {documents.map((doc, idx) => (
                <TableRow key={`${doc.related_document_id ?? doc.document_number}-${idx}`}>
                  <TableCell className="font-mono text-xs">
                    {doc.document_number ?? doc.related_document_id ?? "—"}
                  </TableCell>
                  <TableCell>{doc.client_name || "—"}</TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {formatClp(doc.monto_clp)}
                  </TableCell>
                  <TableCell>
                    <Select
                      disabled={isClosed}
                      value={doc.medio_pago || "pendiente"}
                      onValueChange={(v) =>
                        setDocuments((rows) =>
                          rows.map((r, i) => (i === idx ? { ...r, medio_pago: v } : r)),
                        )
                      }
                    >
                      <SelectTrigger className="h-8 w-[140px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {MEDIOS_PAGO.map((m) => (
                          <SelectItem key={m} value={m}>
                            {MEDIO_PAGO_LABELS[m]}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </TableCell>
                  <TableCell>
                    <Input
                      disabled={isClosed}
                      value={doc.observacion || ""}
                      onChange={(e) =>
                        setDocuments((rows) =>
                          rows.map((r, i) =>
                            i === idx ? { ...r, observacion: e.target.value } : r,
                          ),
                        )
                      }
                      className="h-8"
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Resumen por medio de pago</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {MEDIOS_PAGO.filter((m) => m !== "pendiente").map((m) => (
            <ReadOnlyAmount
              key={m}
              label={MEDIO_PAGO_LABELS[m]}
              value={resultado.resumen_pagos?.[m] ?? 0}
            />
          ))}
        </div>
      </section>

      <EditableSection
        title="Notas de crédito"
        disabled={isClosed}
        onAdd={() => setCreditNotes((r) => [...r, emptyCreditNoteV2Row()])}
      >
        {creditNotes.length === 0 ? (
          <p className="text-xs text-muted-foreground">Sin notas de crédito registradas.</p>
        ) : (
          creditNotes.map((row, idx) => (
            <div
              key={idx}
              className="grid gap-2 rounded-md border border-border/60 p-2 sm:grid-cols-[1fr_1fr_100px_100px_1fr_80px_36px] sm:items-center"
            >
              <Input
                disabled={isClosed}
                placeholder="Doc. venta"
                value={row.documento_venta}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) =>
                      i === idx ? { ...r, documento_venta: e.target.value } : r,
                    ),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="Cliente"
                value={row.cliente}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, cliente: e.target.value } : r)),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="N° NC"
                value={row.numero_nc}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, numero_nc: e.target.value } : r)),
                  )
                }
              />
              <Input
                disabled={isClosed}
                inputMode="numeric"
                placeholder="Monto"
                value={row.monto ? String(row.monto) : ""}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) =>
                      i === idx ? { ...r, monto: parseClpInput(e.target.value) } : r,
                    ),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="Motivo"
                value={row.motivo}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, motivo: e.target.value } : r)),
                  )
                }
              />
              <label className="flex items-center gap-1.5 text-xs">
                <Checkbox
                  disabled={isClosed}
                  checked={row.aplicada}
                  onCheckedChange={(v) =>
                    setCreditNotes((rows) =>
                      rows.map((r, i) => (i === idx ? { ...r, aplicada: v === true } : r)),
                    )
                  }
                />
                Aplicada
              </label>
              {!isClosed ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="size-8"
                  onClick={() => setCreditNotes((rows) => rows.filter((_, i) => i !== idx))}
                >
                  <Trash2 className="size-4" />
                </Button>
              ) : null}
            </div>
          ))
        )}
      </EditableSection>

      <EditableSection
        title="No cargados (por producto)"
        disabled={isClosed}
        onAdd={() => updateNotLoaded([...notLoaded, emptyNotLoadedV2Row()])}
      >
        {notLoaded.length === 0 ? (
          <p className="text-xs text-muted-foreground">Sin productos no cargados.</p>
        ) : (
          notLoaded.map((row, idx) => (
            <div
              key={idx}
              className="grid gap-2 rounded-md border border-border/60 p-2 sm:grid-cols-[1fr_1fr_80px_1fr_100px_36px] sm:items-center"
            >
              <Input
                disabled={isClosed}
                placeholder="Cliente"
                value={row.cliente}
                onChange={(e) =>
                  updateNotLoaded(
                    notLoaded.map((r, i) => (i === idx ? { ...r, cliente: e.target.value } : r)),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="Producto"
                list={`cuadratura-products-${planId}`}
                value={row.producto}
                onChange={(e) =>
                  updateNotLoaded(
                    notLoaded.map((r, i) => (i === idx ? { ...r, producto: e.target.value } : r)),
                  )
                }
              />
              <Input
                disabled={isClosed}
                inputMode="decimal"
                placeholder="Cant."
                value={row.cantidad ? String(row.cantidad) : ""}
                onChange={(e) =>
                  updateNotLoaded(
                    notLoaded.map((r, i) =>
                      i === idx
                        ? { ...r, cantidad: Number(e.target.value) || 0, monto_clp: undefined }
                        : r,
                    ),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="Motivo"
                value={row.motivo}
                onChange={(e) =>
                  updateNotLoaded(
                    notLoaded.map((r, i) => (i === idx ? { ...r, motivo: e.target.value } : r)),
                  )
                }
              />
              <span className="font-mono text-xs">{formatClp(row.monto_clp ?? 0)}</span>
              {!isClosed ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="size-8"
                  onClick={() => updateNotLoaded(notLoaded.filter((_, i) => i !== idx))}
                >
                  <Trash2 className="size-4" />
                </Button>
              ) : null}
            </div>
          ))
        )}
        <datalist id={`cuadratura-products-${planId}`}>
          {catalog.map((p) => (
            <option key={`${p.product_id}-${p.variant_id}-${p.producto}`} value={p.producto} />
          ))}
        </datalist>
        <p className="text-xs text-muted-foreground">
          Monto descontado total: {formatClp(resultado.no_cargados_clp)}
        </p>
      </EditableSection>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Resultado</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          <ReadOnlyAmount label="Venta picking" value={ventaPicking} />
          <ReadOnlyAmount label="(−) Notas crédito" value={-resultado.notas_credito_clp} />
          <ReadOnlyAmount label="(−) No cargados" value={-resultado.no_cargados_clp} />
          <ReadOnlyAmount label="Venta ajustada" value={resultado.venta_ajustada_clp} />
          <ReadOnlyAmount label="Total recaudado" value={resultado.total_recaudado_clp} />
          <div className={cn("rounded-md border p-3", diffStatusClass(resultado.diferencia_status))}>
            <p className="text-[11px] opacity-80">Diferencia</p>
            <p className="font-mono text-sm font-semibold">{formatClp(resultado.diferencia_clp)}</p>
          </div>
        </div>
      </section>

      <section className="space-y-2">
        <Label className="text-sm font-semibold">
          Observación
          {obsRequired ? <span className="text-destructive"> *</span> : null}
        </Label>
        <Textarea
          disabled={isClosed}
          value={observacion}
          onChange={(e) => setObservacion(e.target.value)}
          placeholder={
            obsRequired
              ? "Obligatorio: explique la diferencia operacional"
              : "Opcional hasta cerrar con diferencia"
          }
          rows={3}
        />
      </section>

      {data.history && data.history.length > 0 ? (
        <section className="space-y-2 border-t pt-4">
          <h3 className="text-sm font-semibold">Historial de cierres</h3>
          <div className="space-y-2">
            {data.history.map((h) => (
              <div
                key={h.id}
                className="flex flex-wrap items-center justify-between gap-2 rounded-md border px-3 py-2 text-xs"
              >
                <span>
                  v{h.version} · {h.closed_at?.slice(0, 10) ?? "—"} ·{" "}
                  {operationalStatusBadge(h.status).emoji}{" "}
                  {operationalStatusBadge(h.status).label}
                </span>
                <span className="font-mono">
                  {formatClp(h.diferencia_clp ?? 0)}
                </span>
              </div>
            ))}
          </div>
        </section>
      ) : null}

      {!isClosed ? (
        <div className="flex flex-wrap gap-2 border-t pt-4">
          <Button type="button" onClick={() => void save()} disabled={saving || closing}>
            {saving ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Save className="mr-2 size-4" />}
            Guardar borrador
          </Button>
          <Button type="button" variant="default" onClick={() => void closeCuadratura()} disabled={saving || closing}>
            {closing ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Lock className="mr-2 size-4" />}
            Cerrar cuadratura
          </Button>
          <Button type="button" variant="outline" disabled={saving || closing} onClick={() => void load()}>
            Recargar
          </Button>
        </div>
      ) : null}
    </div>
  )
}

function EditableSection({
  title,
  children,
  onAdd,
  disabled,
}: {
  title: string
  children: ReactNode
  onAdd: () => void
  disabled?: boolean
}) {
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        {!disabled ? (
          <Button type="button" variant="outline" size="sm" onClick={onAdd}>
            <Plus className="mr-1 size-3.5" />
            Agregar fila
          </Button>
        ) : null}
      </div>
      {children}
    </section>
  )
}
