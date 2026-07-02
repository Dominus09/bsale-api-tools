"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { CheckCircle2, Loader2, Scale } from "lucide-react"

import {
  createOrderWeightLogistics,
  getOrderWeight,
  patchOrderWeightProduct,
  type DistribuidoraDispatchPrepPlanningRow,
  type OrderWeightDetail,
  type OrderWeightLine,
} from "@/lib/api"
import {
  logisticsPatchFromUnitKg,
  orderWeightToPlanningPatch,
} from "@/lib/pre-despacho-weight"
import { emitOrderWeightUpdated } from "@/lib/order-weight-events"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"

export type PreDespachoWeightSheetMode = "incomplete" | "full"

type PreDespachoWeightSheetProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  row: DistribuidoraDispatchPrepPlanningRow | null
  mode: PreDespachoWeightSheetMode
  onRowUpdated: (
    documentId: number,
    patch: Partial<DistribuidoraDispatchPrepPlanningRow>,
  ) => void
  onSaved?: () => void
}

function formatKg(n: number | null | undefined, digits = 1): string {
  const v = Number(n)
  if (!Number.isFinite(v)) return "—"
  return `${v.toLocaleString("es-CL", { maximumFractionDigits: digits })} kg`
}

function sinPesoLines(lines: OrderWeightLine[]): OrderWeightLine[] {
  return lines.filter((ln) => ln.estado_linea === "sin_peso" && ln.cantidad_unitaria > 0)
}

type LineEditorProps = {
  line: OrderWeightLine
  draftUnit: string
  onDraftChange: (value: string) => void
  saving: boolean
  onSave: () => void
  isSaved: boolean
  compact?: boolean
}

function WeightLineEditor({
  line,
  draftUnit,
  onDraftChange,
  saving,
  onSave,
  isSaved,
  compact = false,
}: LineEditorProps) {
  const unitNum = Number(draftUnit.replace(",", "."))
  const upb = line.units_per_box != null && line.units_per_box > 0 ? line.units_per_box : null
  const estBox =
    Number.isFinite(unitNum) && unitNum > 0 ? unitNum * (upb ?? 1) : null

  const pesoActual =
    line.peso_linea_kg != null && line.peso_linea_kg > 0
      ? formatKg(line.peso_linea_kg)
      : line.peso_unitario_kg != null && line.peso_unitario_kg > 0
        ? formatKg(line.peso_unitario_kg)
        : "Sin dato"

  return (
    <tr
      className={cn(
        "border-b border-border/50 transition-colors",
        isSaved
          ? "bg-emerald-50/70 dark:bg-emerald-950/20"
          : "bg-amber-50/40 dark:bg-amber-950/15",
      )}
    >
      <td className="px-3 py-3 align-middle font-mono text-[11px] text-muted-foreground whitespace-nowrap">
        {line.codigo || "—"}
      </td>
      <td className="px-3 py-3 align-middle min-w-[16rem] max-w-[32rem]">
        <p className="text-sm font-semibold leading-snug text-foreground whitespace-normal break-words">
          {line.producto || "—"}
        </p>
        {line.variante?.trim() ? (
          <div className="mt-1.5">
            <p className="text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
              Variante
            </p>
            <p className="text-xs leading-snug text-muted-foreground whitespace-normal break-words">
              {line.variante.trim()}
            </p>
          </div>
        ) : null}
      </td>
      <td className="px-3 py-3 align-middle text-right text-sm tabular-nums font-medium">
        {line.cantidad_unitaria}
      </td>
      <td className="px-3 py-3 align-middle text-right text-sm tabular-nums">
        {line.cantidad_cajas != null ? line.cantidad_cajas : "—"}
      </td>
      <td className="px-3 py-3 align-middle text-right text-sm tabular-nums text-muted-foreground">
        {pesoActual}
      </td>
      <td className="px-3 py-3 align-middle w-[9.5rem]">
        {isSaved ? (
          <span className="flex items-center justify-end gap-1.5 text-sm font-medium text-emerald-700 dark:text-emerald-400">
            <CheckCircle2 className="size-4 shrink-0" aria-hidden />
            Peso actualizado
          </span>
        ) : (
          <div className="space-y-1">
            <Input
              type="number"
              step="0.001"
              min={0}
              disabled={saving}
              className="h-10 w-full text-right text-sm tabular-nums font-medium"
              value={draftUnit}
              onChange={(e) => onDraftChange(e.target.value)}
              placeholder="kg unit."
              aria-label="Nuevo peso unitario en kg"
            />
            {estBox != null ? (
              <p className="text-[10px] text-right text-muted-foreground tabular-nums">
                Caja est.: {estBox.toLocaleString("es-CL", { maximumFractionDigits: 2 })} kg
              </p>
            ) : null}
          </div>
        )}
      </td>
      {!compact ? (
        <td className="px-3 py-3 align-middle w-[11rem]">
          {isSaved ? (
            <div className="flex h-full min-h-[3.25rem] items-center justify-center rounded-md border border-emerald-200/80 bg-emerald-100/50 px-2 text-center text-xs font-semibold text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-300">
              🟢 Listo
            </div>
          ) : (
            <Button
              type="button"
              className="h-full min-h-[3.25rem] w-full text-sm font-semibold shadow-sm"
              disabled={saving || !Number.isFinite(unitNum) || unitNum <= 0}
              onClick={onSave}
            >
              {saving ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <>💾 Guardar peso</>
              )}
            </Button>
          )}
        </td>
      ) : null}
    </tr>
  )
}

function HeaderKpi({
  label,
  value,
  sub,
}: {
  label: string
  value: string
  sub?: string
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-muted/25 px-3 py-2.5">
      <p className="text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
        {label}
      </p>
      <p className="mt-0.5 text-base font-semibold tabular-nums text-foreground">{value}</p>
      {sub ? <p className="text-[10px] text-muted-foreground">{sub}</p> : null}
    </div>
  )
}

export function PreDespachoWeightSheet({
  open,
  onOpenChange,
  row,
  mode,
  onRowUpdated,
  onSaved,
}: PreDespachoWeightSheetProps) {
  const [order, setOrder] = useState<OrderWeightDetail | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [savingLineId, setSavingLineId] = useState<number | null>(null)
  const [draftUnits, setDraftUnits] = useState<Record<number, string>>({})
  const [trackedLines, setTrackedLines] = useState<OrderWeightLine[]>([])
  const [savedLineIds, setSavedLineIds] = useState<Set<number>>(() => new Set())
  const [allComplete, setAllComplete] = useState(false)
  const autoCloseRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const documentId = row?.document_id ?? null

  const clearAutoClose = useCallback(() => {
    if (autoCloseRef.current) {
      clearTimeout(autoCloseRef.current)
      autoCloseRef.current = null
    }
  }, [])

  const loadOrder = useCallback(
    async (docId: number, sheetMode: PreDespachoWeightSheetMode) => {
      setLoading(true)
      setError(null)
      setAllComplete(false)
      try {
        const data = await getOrderWeight(docId)
        console.info(
          `[POPUP_WEIGHT] order_id=${data.document_id} total_weight=${data.peso_total_kg} coverage=${data.porcentaje_cobertura} missing=${data.productos_sin_peso}`,
        )
        setOrder(data)
        const pending = sinPesoLines(data.lines)
        if (sheetMode === "incomplete") {
          setTrackedLines(pending)
          setSavedLineIds(new Set())
        }
        const drafts: Record<number, string> = {}
        for (const ln of sheetMode === "incomplete" ? pending : data.lines) {
          if (ln.estado_linea === "sin_peso" && ln.cantidad_unitaria > 0) {
            drafts[ln.detail_id] = ""
          }
        }
        setDraftUnits(drafts)
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Error al cargar peso")
        setOrder(null)
        setTrackedLines([])
      } finally {
        setLoading(false)
      }
    },
    [],
  )

  useEffect(() => {
    if (open && documentId != null) {
      void loadOrder(documentId, mode)
    }
    if (!open) {
      clearAutoClose()
      setOrder(null)
      setError(null)
      setDraftUnits({})
      setSavingLineId(null)
      setTrackedLines([])
      setSavedLineIds(new Set())
      setAllComplete(false)
    }
  }, [open, documentId, mode, loadOrder, clearAutoClose])

  const freshLineById = useMemo(() => {
    const map = new Map<number, OrderWeightLine>()
    for (const ln of order?.lines ?? []) {
      map.set(ln.detail_id, ln)
    }
    return map
  }, [order?.lines])

  const isLineSaved = useCallback(
    (line: OrderWeightLine) => {
      if (savedLineIds.has(line.detail_id)) return true
      const fresh = freshLineById.get(line.detail_id)
      return fresh != null && fresh.estado_linea !== "sin_peso"
    },
    [savedLineIds, freshLineById],
  )

  const displayLines = useMemo(() => {
    if (mode === "incomplete") {
      return trackedLines.map((ln) => freshLineById.get(ln.detail_id) ?? ln)
    }
    return (order?.lines ?? []).filter((ln) => ln.cantidad_unitaria > 0)
  }, [mode, trackedLines, freshLineById, order?.lines])

  const pendientes = order?.productos_sin_peso ?? row?.productos_sin_peso ?? 0
  const cobertura = order?.porcentaje_cobertura ?? row?.porcentaje_cobertura_peso ?? 0
  const pesoActual = order?.peso_total_kg ?? row?.peso_total_kg ?? row?.weight_kg

  const applyOrderUpdate = useCallback(
    (detail: OrderWeightDetail) => {
      setOrder(detail)
      if (row) {
        const patch = orderWeightToPlanningPatch(detail)
        onRowUpdated(row.document_id, patch)
        emitOrderWeightUpdated(row.document_id, patch)
      }
    },
    [onRowUpdated, row],
  )

  useEffect(() => {
    if (!open || mode !== "incomplete" || loading || !order) return
    if (pendientes === 0 && trackedLines.length > 0) {
      setAllComplete(true)
      clearAutoClose()
      autoCloseRef.current = setTimeout(() => {
        onOpenChange(false)
      }, 1000)
      return () => clearAutoClose()
    }
    setAllComplete(false)
  }, [
    open,
    mode,
    loading,
    order,
    pendientes,
    trackedLines.length,
    onOpenChange,
    clearAutoClose,
  ])

  const saveLine = async (line: OrderWeightLine) => {
    if (!order || isLineSaved(line)) return
    const raw = draftUnits[line.detail_id] ?? ""
    const unitKg = Number(raw.replace(",", "."))
    if (!Number.isFinite(unitKg) || unitKg <= 0) return

    setSavingLineId(line.detail_id)
    setError(null)
    try {
      const patchBody = logisticsPatchFromUnitKg(unitKg, line.units_per_box)
      const pmId = line.products_master_id

      let updated: OrderWeightDetail
      if (!pmId && line.variant_id) {
        const created = await createOrderWeightLogistics(line.variant_id, order.document_id)
        const newPmId = Number(created.product?.id)
        if (!newPmId) {
          updated = created.order
        } else {
          const res = await patchOrderWeightProduct(newPmId, order.document_id, patchBody)
          updated = res.order
        }
      } else if (pmId) {
        const res = await patchOrderWeightProduct(pmId, order.document_id, patchBody)
        updated = res.order
      } else {
        throw new Error("Sin ficha logística para este producto")
      }

      setSavedLineIds((prev) => new Set(prev).add(line.detail_id))
      applyOrderUpdate(updated)
      setDraftUnits((prev) => {
        const next = { ...prev }
        delete next[line.detail_id]
        return next
      })
      onSaved?.()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al guardar peso")
    } finally {
      setSavingLineId(null)
    }
  }

  const title =
    mode === "incomplete"
      ? `Peso incompleto — OC ${row?.oc ?? row?.document_id ?? ""}`
      : `Peso de la orden — OC ${row?.oc ?? row?.document_id ?? ""}`

  const showSaveColumn = mode === "incomplete" || displayLines.some((ln) => !isLineSaved(ln))

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        showCloseButton
        className={cn(
          "flex max-h-[85vh] w-[min(1050px,calc(100vw-1.5rem))] max-w-[1050px] flex-col gap-0 overflow-hidden p-0",
          "sm:max-w-[min(1050px,calc(100vw-1.5rem))]",
        )}
      >
        <DialogHeader className="shrink-0 space-y-3 border-b border-border/70 px-6 py-5 text-left">
          <DialogTitle className="flex items-center gap-2 text-lg">
            <Scale className="size-5 text-primary" aria-hidden />
            {title}
          </DialogTitle>
          <DialogDescription asChild>
            <div className="space-y-3">
              <p className="text-sm font-medium text-foreground">
                {row?.nombre_fantasia?.trim() || "—"}
              </p>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                <HeaderKpi
                  label="OC"
                  value={String(row?.oc ?? row?.document_id ?? "—")}
                />
                <HeaderKpi label="Peso actual" value={formatKg(pesoActual)} />
                <HeaderKpi
                  label="Cobertura"
                  value={`${Number(cobertura).toLocaleString("es-CL", { maximumFractionDigits: 0 })}%`}
                />
                <HeaderKpi
                  label="Pendientes"
                  value={`${pendientes} producto${pendientes === 1 ? "" : "s"}`}
                  sub={
                    mode === "incomplete" && pendientes > 0
                      ? "Corrija cada línea y guarde"
                      : undefined
                  }
                />
              </div>
            </div>
          </DialogDescription>
        </DialogHeader>

        <div className="flex min-h-0 flex-1 flex-col">
          {loading ? (
            <div className="flex flex-1 items-center justify-center gap-2 py-20 text-sm text-muted-foreground">
              <Loader2 className="size-5 animate-spin" />
              Cargando productos…
            </div>
          ) : (
            <>
              {error ? (
                <div className="shrink-0 px-6 pt-4">
                  <p className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                    {error}
                  </p>
                </div>
              ) : null}

              {allComplete ? (
                <div className="shrink-0 mx-6 mt-4 flex items-center justify-between gap-3 rounded-lg border border-emerald-300/60 bg-emerald-50 px-4 py-3 dark:border-emerald-800 dark:bg-emerald-950/30">
                  <p className="text-sm font-medium text-emerald-800 dark:text-emerald-200">
                    🟢 Peso completo — todos los productos tienen peso asignado.
                  </p>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="shrink-0 border-emerald-400 text-emerald-800"
                    onClick={() => onOpenChange(false)}
                  >
                    Finalizar
                  </Button>
                </div>
              ) : null}

              <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4">
                {displayLines.length === 0 ? (
                  <p className="py-16 text-center text-sm text-muted-foreground">
                    {mode === "incomplete"
                      ? "No hay productos pendientes de peso en esta orden."
                      : "Sin líneas de producto."}
                  </p>
                ) : (
                  <div className="overflow-x-auto rounded-lg border border-border/80 shadow-sm">
                    <table className="w-full min-w-[800px] text-left">
                      <thead className="sticky top-0 z-10 bg-muted/90 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground backdrop-blur-sm">
                        <tr>
                          <th className="px-3 py-2.5 w-[5.5rem]">Código</th>
                          <th className="px-3 py-2.5 min-w-[16rem]">Producto</th>
                          <th className="px-3 py-2.5 text-right w-[5rem]">Cant. unit.</th>
                          <th className="px-3 py-2.5 text-right w-[4.5rem]">Cajas</th>
                          <th className="px-3 py-2.5 text-right w-[6rem]">Peso actual</th>
                          <th className="px-3 py-2.5 w-[9.5rem]">Nuevo peso</th>
                          {showSaveColumn ? (
                            <th className="px-3 py-2.5 w-[11rem] text-center">Acción</th>
                          ) : null}
                        </tr>
                      </thead>
                      <tbody>
                        {displayLines.map((line) => {
                          const saved = isLineSaved(line)
                          return (
                            <WeightLineEditor
                              key={line.detail_id}
                              line={line}
                              draftUnit={draftUnits[line.detail_id] ?? ""}
                              onDraftChange={(v) =>
                                setDraftUnits((prev) => ({ ...prev, [line.detail_id]: v }))
                              }
                              saving={savingLineId === line.detail_id}
                              onSave={() => void saveLine(line)}
                              isSaved={saved}
                              compact={!showSaveColumn}
                            />
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                )}

                {mode === "full" && order ? (
                  <div className="mt-4 grid grid-cols-2 gap-2 text-xs sm:grid-cols-4">
                    <HeaderKpi
                      label="Con peso"
                      value={String(order.productos_con_peso)}
                    />
                    <HeaderKpi
                      label="Sin peso"
                      value={String(order.productos_sin_peso)}
                    />
                    <HeaderKpi
                      label="Manuales"
                      value={String(order.productos_manuales)}
                    />
                    <HeaderKpi
                      label="Estimados"
                      value={String(order.productos_estimados)}
                    />
                  </div>
                ) : null}
              </div>
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
