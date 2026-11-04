"use client"

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react"
import Link from "next/link"
import { AlertTriangle, Loader2, Lock, Plus, Save, Trash2 } from "lucide-react"

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
  defaultCashCount,
  diffStatusClass,
  documentCollectionPresentation,
  emptyCreditNoteV2Row,
  emptyNotLoadedV2Row,
  normalizeCashCount,
  normalizeNotLoadedRow,
  normalizeNotLoadedRows,
  observacionRequired,
  operationalStatusBadge,
  type CuadraturaCashCountRow,
  type CuadraturaCreditNoteV2Row,
  type CuadraturaDocumentRow,
  type CuadraturaNotLoadedV2Row,
  type CuadraturaProductCatalogRow,
} from "@/lib/dispatch-plan-cuadratura"
import { formatClp } from "@/lib/ors-map-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
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

function normalizeCreditNotesFromApi(
  rows: Record<string, unknown>[] | undefined,
): CuadraturaCreditNoteV2Row[] {
  return (rows ?? []).map((row) => ({
    documento: String(row.documento ?? row.documento_venta ?? ""),
    nota_credito: String(row.nota_credito ?? row.numero_nc ?? ""),
    monto: Math.round(Number(row.monto) || 0),
    observacion: String(row.observacion ?? row.motivo ?? ""),
  }))
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
  const [cashCount, setCashCount] = useState<CuadraturaCashCountRow[]>(defaultCashCount())
  const [catalog, setCatalog] = useState<CuadraturaProductCatalogRow[]>([])
  const [observacion, setObservacion] = useState("")

  const isClosed = Boolean(data?.closed_at)
  const cuadraturaStarted = Boolean(data?.cuadratura_started ?? data?.persisted)
  const ventaPicking = data?.ventas.venta_picking_clp ?? 0

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await getDispatchPlanCuadratura(planId)
      setData(res)
      setDocuments((res.documents ?? []) as CuadraturaDocumentRow[])
      setCreditNotes(normalizeCreditNotesFromApi(res.credit_notes_v2 as Record<string, unknown>[]))
      const cat = (res.product_catalog ?? []) as CuadraturaProductCatalogRow[]
      setCatalog(cat)
      setNotLoaded(normalizeNotLoadedRows((res.not_loaded_v2 ?? []) as CuadraturaNotLoadedV2Row[], cat))
      setCashCount(normalizeCashCount(res.cash_count ?? defaultCashCount()))
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
        cash_count: cashCount,
      }),
    [ventaPicking, documents, creditNotes, cashCount],
  )

  const obsRequired = observacionRequired(resultado)
  const statusBadge = operationalStatusBadge(
    !cuadraturaStarted
      ? "not_started"
      : data?.operational_status || "pending",
  )
  const cashDiff = resultado.diferencia_efectivo_clp ?? 0

  const persistPayload = () => ({
    schema_version: 2 as const,
    observacion: observacion.trim() || null,
    documents,
    credit_notes_v2: creditNotes,
    not_loaded_v2: notLoaded,
    cash_count: normalizeCashCount(cashCount),
  })

  const save = async () => {
    if (obsRequired && !observacion.trim()) {
      onMessage("Debe ingresar una observación cuando hay diferencia general o de efectivo.")
      return
    }
    setSaving(true)
    try {
      await putDispatchPlanCuadratura(planId, persistPayload())
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
      await putDispatchPlanCuadratura(planId, persistPayload())
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
        Se requiere picking generado para cuadratura documental.
      </p>
    )
  }

  const hdr = (data.header || {}) as Record<string, unknown>
  const docsEntregados = documents.filter((d) => (d.medio_pago || "pendiente") !== "pendiente").length
  const docsPendientes = documents.filter((d) => (d.medio_pago || "pendiente") === "pendiente").length
  const ncTotal = creditNotes.reduce((a, r) => a + Math.round(Number(r.monto) || 0), 0)
  const saldoPendiente =
    (resultado.resumen_pagos?.pendiente ?? 0) ||
    documents
      .filter((d) => (d.medio_pago || "pendiente") === "pendiente")
      .reduce((a, d) => a + Math.round(Number(d.monto_clp) || 0), 0)

  return (
    <div className="space-y-8">
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

      {!cuadraturaStarted ? (
        <Alert className="border-amber-500/40 bg-amber-500/10">
          <AlertTriangle className="size-4" />
          <AlertTitle>Cuadratura no iniciada</AlertTitle>
          <AlertDescription className="text-xs">
            Aún no hay registro de cierre de ruta. Los montos comerciales son referencia del
            despacho (no cobro registrado). Guardar inicia el cierre; no se crea al abrir esta
            página.
          </AlertDescription>
        </Alert>
      ) : null}

      {/* A. Resumen del despacho */}
      <section className="space-y-3 rounded-lg border bg-muted/10 p-4">
        <h3 className="text-sm font-semibold tracking-tight">A. Resumen del despacho</h3>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 text-sm">
          <MetaField
            label="Planificación"
            value={String(hdr.planning_number || hdr.planning_name || `Plan ${planId}`)}
          />
          <MetaField label="Camión" value={String(hdr.truck_name || "—")} />
          <MetaField
            label="Chofer"
            value={String(hdr.driver_name || hdr.driver_label || "—")}
          />
          <MetaField label="Fecha" value={String(hdr.delivery_date || "—")} />
          <MetaField label="Documentos" value={String(documents.length)} />
          <MetaField label="Venta documentada" value={formatClp(ventaPicking)} />
          <MetaField label="Estado de cierre" value={statusBadge.label} />
          <MetaField
            label="Base montos"
            value={data.ventas.amounts_basis_label_es || "Total documento (con IVA)"}
          />
        </div>
        <div className="grid gap-2 sm:grid-cols-3">
          <ReadOnlyAmount label="Venta OC (referencia)" value={data.ventas.venta_oc_clp} />
          <ReadOnlyAmount
            label="Venta facturada (referencia)"
            value={data.ventas.venta_facturada_clp}
          />
          <ReadOnlyAmount label="Venta picking (documentada)" value={ventaPicking} />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Venta facturada ≠ dinero recibido. No se mezclan en el cierre.
        </p>
      </section>

      {/* B. Control documental */}
      <section className="space-y-3">
        <h3 className="text-sm font-semibold tracking-tight">B. Control documental</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
          <StatCard label="Documentos en ruta" value={String(documents.length)} />
          <StatCard label="Con medio registrado" value={String(docsEntregados)} />
          <StatCard label="Pendientes de cobro" value={String(docsPendientes)} />
          <ReadOnlyAmount label="Notas de crédito (monto)" value={ncTotal} />
          <ReadOnlyAmount label="Saldo pendiente" value={saldoPendiente} />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Devueltos / no entregados se registran vía NC y «No cargados». Sin ceros engañosos:
          pendiente muestra el monto real aún no asignado a un medio.
        </p>
      </section>

      {/* C. Registro de cobranzas */}
      <section className="space-y-2">
        <div>
          <h3 className="text-sm font-semibold tracking-tight">C. Registro de cobranzas</h3>
          <p className="text-xs text-muted-foreground">
            Modelo actual: un medio por documento. La UI ya reserva columnas para pago mixto
            (E4); la persistencia multi-medio no está activa.
          </p>
        </div>
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Cliente</TableHead>
                <TableHead>Documento</TableHead>
                <TableHead className="text-right">Por cobrar</TableHead>
                <TableHead>Medio</TableHead>
                <TableHead className="text-right">Recibido</TableHead>
                <TableHead className="text-right">Diferencia</TableHead>
                <TableHead>Observaciones</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {documents.map((doc, idx) => {
                const coll = documentCollectionPresentation(doc)
                return (
                  <TableRow key={`${doc.related_document_id ?? doc.document_number}-${idx}`}>
                    <TableCell className="text-xs">{doc.client_name || "—"}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {doc.document_number ?? doc.related_document_id ?? "—"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {formatClp(coll.monto_por_cobrar)}
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
                      <p className="mt-0.5 text-[10px] text-muted-foreground">
                        Mixto E4: deshabilitado
                      </p>
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {formatClp(coll.monto_recibido)}
                    </TableCell>
                    <TableCell
                      className={cn(
                        "text-right font-mono text-xs",
                        coll.diferencia !== 0 && "text-amber-700 dark:text-amber-300",
                      )}
                    >
                      {formatClp(coll.diferencia)}
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
                )
              })}
            </TableBody>
          </Table>
        </div>
      </section>

      {/* D. Resumen de dinero */}
      <section className="space-y-2">
        <h3 className="text-sm font-semibold tracking-tight">D. Resumen de dinero</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {MEDIOS_PAGO.filter((m) => m !== "pendiente").map((m) => (
            <ReadOnlyAmount
              key={m}
              label={MEDIO_PAGO_LABELS[m]}
              value={resultado.resumen_pagos?.[m] ?? 0}
            />
          ))}
          <ReadOnlyAmount
            label="Total cobrado (medios)"
            value={resultado.total_recaudado_documental_clp ?? resultado.total_recaudado_clp}
          />
          <ReadOnlyAmount label="Saldo pendiente" value={saldoPendiente} />
        </div>
      </section>

      <EditableSection
        title="Notas de crédito"
        hint="Descuentan de la venta ajustada. Use NC cuando corresponda descontar valor por no entrega/devolución."
        disabled={isClosed}
        onAdd={() => setCreditNotes((r) => [...r, emptyCreditNoteV2Row()])}
      >
        {creditNotes.length === 0 ? (
          <p className="text-xs text-muted-foreground">Sin notas de crédito registradas.</p>
        ) : (
          creditNotes.map((row, idx) => (
            <div
              key={idx}
              className="grid gap-2 rounded-md border border-border/60 p-2 sm:grid-cols-[1fr_1fr_110px_1fr_36px] sm:items-center"
            >
              <Input
                disabled={isClosed}
                placeholder="N° documento"
                value={row.documento}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, documento: e.target.value } : r)),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="N° nota crédito"
                value={row.nota_credito}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, nota_credito: e.target.value } : r)),
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
                placeholder="Observación"
                value={row.observacion}
                onChange={(e) =>
                  setCreditNotes((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, observacion: e.target.value } : r)),
                  )
                }
              />
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
        title="No cargados (control operacional)"
        hint="Solo registro de productos faltantes. El descuento monetario va en Notas de crédito."
        disabled={isClosed}
        onAdd={() => setNotLoaded((r) => [...r, emptyNotLoadedV2Row()])}
      >
        {notLoaded.length === 0 ? (
          <p className="text-xs text-muted-foreground">Sin productos no cargados.</p>
        ) : (
          notLoaded.map((row, idx) => (
            <div
              key={idx}
              className="grid gap-2 rounded-md border border-border/60 p-2 sm:grid-cols-[2fr_80px_1fr_36px] sm:items-center"
            >
              <div className="space-y-1">
                <Input
                  disabled={isClosed}
                  placeholder="Producto o código de barras"
                  list={`cuadratura-products-${planId}`}
                  value={row.producto}
                  onChange={(e) =>
                    setNotLoaded((rows) =>
                      rows.map((r, i) =>
                        i === idx ? normalizeNotLoadedRow({ ...r, producto: e.target.value }, catalog) : r,
                      ),
                    )
                  }
                  onBlur={(e) =>
                    setNotLoaded((rows) =>
                      rows.map((r, i) =>
                        i === idx ? normalizeNotLoadedRow({ ...r, producto: e.target.value }, catalog) : r,
                      ),
                    )
                  }
                />
                {row.producto_variante && row.producto_variante !== row.producto ? (
                  <p className="text-[11px] text-muted-foreground">{row.producto_variante}</p>
                ) : null}
              </div>
              <Input
                disabled={isClosed}
                inputMode="decimal"
                placeholder="Cant."
                value={row.cantidad ? String(row.cantidad) : ""}
                onChange={(e) =>
                  setNotLoaded((rows) =>
                    rows.map((r, i) =>
                      i === idx ? { ...r, cantidad: Number(e.target.value) || 0 } : r,
                    ),
                  )
                }
              />
              <Input
                disabled={isClosed}
                placeholder="Motivo"
                value={row.motivo}
                onChange={(e) =>
                  setNotLoaded((rows) =>
                    rows.map((r, i) => (i === idx ? { ...r, motivo: e.target.value } : r)),
                  )
                }
              />
              {!isClosed ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="size-8"
                  onClick={() => setNotLoaded((rows) => rows.filter((_, i) => i !== idx))}
                >
                  <Trash2 className="size-4" />
                </Button>
              ) : null}
            </div>
          ))
        )}
        <datalist id={`cuadratura-products-${planId}`}>
          {catalog.flatMap((p) => [
            <option
              key={`${p.product_id}-${p.variant_id}-label`}
              value={p.producto_variante}
              label={p.codigo_barras ? `CB ${p.codigo_barras}` : undefined}
            />,
            ...(p.codigo_barras
              ? [
                  <option
                    key={`${p.product_id}-${p.variant_id}-bc`}
                    value={p.codigo_barras}
                    label={p.producto_variante}
                  />,
                ]
              : []),
          ])}
        </datalist>
      </EditableSection>

      {/* E. Arqueo de efectivo */}
      <section className="space-y-2">
        <h3 className="text-sm font-semibold tracking-tight">E. Arqueo de efectivo</h3>
        <p className="text-xs text-muted-foreground">
          Compare el contado solo contra documentos marcados como Efectivo (no contra venta
          facturada).
        </p>
        <div className="grid gap-2 sm:grid-cols-3">
          <ReadOnlyAmount
            label="Efectivo esperado (documental)"
            value={resultado.total_efectivo_documental_clp ?? 0}
          />
          <ReadOnlyAmount
            label="Efectivo contado"
            value={resultado.total_efectivo_contado_clp ?? 0}
          />
          <ReadOnlyAmount label="Diferencia efectivo" value={cashDiff} />
        </div>
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Denominación</TableHead>
                <TableHead className="w-28">Cantidad</TableHead>
                <TableHead className="text-right">Subtotal</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {normalizeCashCount(cashCount).map((row, idx) => (
                <TableRow key={row.denominacion_clp}>
                  <TableCell className="font-mono text-xs">
                    {formatClp(row.denominacion_clp)}
                  </TableCell>
                  <TableCell>
                    <Input
                      disabled={isClosed}
                      inputMode="numeric"
                      value={row.cantidad ? String(row.cantidad) : ""}
                      onChange={(e) => {
                        const qty = Math.max(0, parseInt(e.target.value || "0", 10) || 0)
                        setCashCount((rows) =>
                          rows.map((r, i) =>
                            i === idx
                              ? {
                                  ...r,
                                  cantidad: qty,
                                  subtotal_clp: r.denominacion_clp * qty,
                                }
                              : r,
                          ),
                        )
                      }}
                      className="h-8"
                    />
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {formatClp(row.subtotal_clp)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <ReadOnlyAmount
          label="Total efectivo contado"
          value={resultado.total_efectivo_contado_clp ?? 0}
        />
      </section>

      {cashDiff !== 0 ? (
        <Alert variant="destructive">
          <AlertTriangle className="size-4" />
          <AlertTitle>Diferencia de efectivo</AlertTitle>
          <AlertDescription>
            Documentos en efectivo: {formatClp(resultado.total_efectivo_documental_clp ?? 0)} ·
            Contado: {formatClp(resultado.total_efectivo_contado_clp ?? 0)} · Diferencia:{" "}
            {formatClp(cashDiff)}
          </AlertDescription>
        </Alert>
      ) : null}

      {/* F. Cierre */}
      <section className="space-y-4 rounded-lg border-2 border-primary/30 bg-primary/5 p-4">
        <h3 className="text-sm font-semibold tracking-tight">F. Cierre</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          <ReadOnlyAmount label="Venta picking" value={ventaPicking} />
          <ReadOnlyAmount label="(−) Notas crédito" value={-resultado.notas_credito_clp} />
          <ReadOnlyAmount label="Venta ajustada" value={resultado.venta_ajustada_clp} />
          <ReadOnlyAmount
            label="Total cobrado (documental)"
            value={resultado.total_recaudado_documental_clp ?? resultado.total_recaudado_clp}
          />
          <ReadOnlyAmount label="Diferencia efectivo" value={cashDiff} />
          <div
            className={cn(
              "rounded-md border p-3",
              diffStatusClass(resultado.diferencia_status),
            )}
          >
            <p className="text-[11px] opacity-80">Diferencia general</p>
            <p className="font-mono text-sm font-semibold">
              {formatClp(resultado.diferencia_general_clp ?? resultado.diferencia_clp)}
            </p>
          </div>
        </div>

        <div className="space-y-2">
          <Label className="text-sm font-semibold">
            Observaciones
            {obsRequired ? <span className="text-destructive"> *</span> : null}
          </Label>
          <Textarea
            disabled={isClosed}
            value={observacion}
            onChange={(e) => setObservacion(e.target.value)}
            placeholder={
              obsRequired
                ? "Obligatorio: explique diferencia general y/o de efectivo"
                : "Opcional"
            }
            rows={3}
          />
        </div>

        <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
          <span>
            Estado: <strong className="text-foreground">{statusBadge.label}</strong>
          </span>
          {data.closed_by ? (
            <span>
              Responsable cierre: <strong className="text-foreground">{data.closed_by}</strong>
            </span>
          ) : null}
          {data.closed_at ? (
            <span>
              Cerrada: <strong className="text-foreground">{data.closed_at.slice(0, 16)}</strong>
            </span>
          ) : null}
        </div>

        {!isClosed ? (
          <div className="flex flex-wrap gap-2 pt-2">
            <Button type="button" variant="secondary" onClick={() => void save()} disabled={saving || closing}>
              {saving ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Save className="mr-2 size-4" />}
              Guardar borrador
            </Button>
            <Button
              type="button"
              size="lg"
              className="min-w-[200px] font-semibold shadow-md"
              onClick={() => void closeCuadratura()}
              disabled={saving || closing}
            >
              {closing ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Lock className="mr-2 size-4" />}
              Cerrar cuadratura
            </Button>
            <Button type="button" variant="outline" disabled={saving || closing} onClick={() => void load()}>
              Recargar
            </Button>
          </div>
        ) : null}
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
                <span className="font-mono">{formatClp(h.diferencia_clp ?? 0)}</span>
              </div>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  )
}

function MetaField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <p className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="truncate text-sm font-medium">{value}</p>
    </div>
  )
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border/60 bg-muted/20 p-3">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p className="font-mono text-sm font-semibold tabular-nums">{value}</p>
    </div>
  )
}

function EditableSection({
  title,
  hint,
  children,
  onAdd,
  disabled,
}: {
  title: string
  hint?: string
  children: ReactNode
  onAdd: () => void
  disabled?: boolean
}) {
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          {hint ? <p className="text-xs text-muted-foreground">{hint}</p> : null}
        </div>
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
