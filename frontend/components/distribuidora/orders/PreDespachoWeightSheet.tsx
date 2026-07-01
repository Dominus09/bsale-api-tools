"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, Scale } from "lucide-react"

import {
  createOrderWeightLogistics,
  getOrderWeight,
  patchOrderWeightProduct,
  type DistribuidoraDispatchPrepPlanningRow,
  type OrderWeightDetail,
  type OrderWeightLine,
} from "@/lib/api"
import {
  formatFuentePeso,
  logisticsPatchFromUnitKg,
  orderWeightToPlanningPatch,
  resolvePreDespachoWeightBadge,
  weightBadgeClass,
  weightBadgeEmoji,
} from "@/lib/pre-despacho-weight"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
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

function formatKg(n: number | null | undefined): string {
  const v = Number(n)
  if (!Number.isFinite(v)) return "—"
  return `${v.toLocaleString("es-CL", { maximumFractionDigits: 1 })} kg`
}

function lineEstadoClass(estado: string): string {
  if (estado === "sin_peso") return "bg-amber-50/80 dark:bg-amber-950/20"
  if (estado === "manual") return "bg-yellow-50/60 dark:bg-yellow-950/15"
  if (estado === "estimado") return "bg-sky-50/60 dark:bg-sky-950/15"
  return ""
}

type LineEditorProps = {
  line: OrderWeightLine
  draftUnit: string
  onDraftChange: (value: string) => void
  saving: boolean
  onSave: () => void
  editable: boolean
}

function WeightLineEditor({
  line,
  draftUnit,
  onDraftChange,
  saving,
  onSave,
  editable,
}: LineEditorProps) {
  const unitNum = Number(draftUnit.replace(",", "."))
  const upb = line.units_per_box != null && line.units_per_box > 0 ? line.units_per_box : null
  const estBox =
    Number.isFinite(unitNum) && unitNum > 0
      ? unitNum * (upb ?? 1)
      : null

  return (
    <tr className={cn("border-b border-border/50", lineEstadoClass(line.estado_linea))}>
      <td className="px-2 py-2 font-mono text-[10px]">{line.codigo || "—"}</td>
      <td className="px-2 py-2 max-w-[8rem]">
        <p className="truncate font-medium">{line.producto || "—"}</p>
        {line.variante ? (
          <p className="truncate text-[10px] text-muted-foreground">{line.variante}</p>
        ) : null}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">{line.cantidad_unitaria}</td>
      <td className="px-2 py-2 text-right tabular-nums">
        {line.cantidad_cajas != null ? line.cantidad_cajas : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">
        {line.peso_unitario_kg != null && line.peso_unitario_kg > 0
          ? formatKg(line.peso_unitario_kg)
          : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">
        {line.peso_caja_kg != null && line.peso_caja_kg > 0 ? formatKg(line.peso_caja_kg) : "—"}
      </td>
      <td className="px-2 py-2">
        {editable ? (
          <div className="space-y-1">
            <Input
              type="number"
              step="0.001"
              min={0}
              className="h-8 w-24 text-right text-xs tabular-nums"
              value={draftUnit}
              onChange={(e) => onDraftChange(e.target.value)}
              placeholder="kg"
              aria-label="Nuevo peso unitario"
            />
            {estBox != null ? (
              <p className="text-[10px] text-muted-foreground tabular-nums">
                Caja est.: {estBox.toLocaleString("es-CL", { maximumFractionDigits: 2 })} kg
              </p>
            ) : null}
          </div>
        ) : (
          <span className="text-right text-xs tabular-nums">
            {line.peso_unitario_kg != null && line.peso_unitario_kg > 0
              ? formatKg(line.peso_unitario_kg)
              : "—"}
          </span>
        )}
      </td>
      <td className="px-2 py-2 text-[10px] text-muted-foreground">
        {formatFuentePeso(line.fuente_peso)}
      </td>
      <td className="px-2 py-2 text-right">
        {editable ? (
          <Button
            type="button"
            size="sm"
            variant="secondary"
            className="h-7 text-[10px]"
            disabled={saving || !Number.isFinite(unitNum) || unitNum <= 0}
            onClick={onSave}
          >
            {saving ? <Loader2 className="size-3 animate-spin" /> : "Guardar peso"}
          </Button>
        ) : (
          <span className="text-[10px] capitalize text-muted-foreground">
            {line.estado_linea.replace(/_/g, " ")}
          </span>
        )}
      </td>
    </tr>
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

  const documentId = row?.document_id ?? null

  const loadOrder = useCallback(async (docId: number) => {
    setLoading(true)
    setError(null)
    try {
      const data = await getOrderWeight(docId)
      setOrder(data)
      const drafts: Record<number, string> = {}
      for (const ln of data.lines) {
        if (ln.estado_linea === "sin_peso" && ln.cantidad_unitaria > 0) {
          drafts[ln.detail_id] = ""
        }
      }
      setDraftUnits(drafts)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar peso")
      setOrder(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (open && documentId != null) {
      void loadOrder(documentId)
    }
    if (!open) {
      setOrder(null)
      setError(null)
      setDraftUnits({})
      setSavingLineId(null)
    }
  }, [open, documentId, loadOrder])

  const visibleLines = useMemo(() => {
    const lines = order?.lines ?? []
    if (mode === "incomplete") {
      return lines.filter((ln) => ln.estado_linea === "sin_peso" && ln.cantidad_unitaria > 0)
    }
    return lines.filter((ln) => ln.cantidad_unitaria > 0)
  }, [order?.lines, mode])

  const headerBadge = row ? resolvePreDespachoWeightBadge(row) : null

  const applyOrderUpdate = (detail: OrderWeightDetail) => {
    setOrder(detail)
    if (row) {
      onRowUpdated(row.document_id, orderWeightToPlanningPatch(detail))
    }
    onSaved?.()
  }

  const saveLine = async (line: OrderWeightLine) => {
    if (!order) return
    const raw = draftUnits[line.detail_id] ?? ""
    const unitKg = Number(raw.replace(",", "."))
    if (!Number.isFinite(unitKg) || unitKg <= 0) return

    setSavingLineId(line.detail_id)
    setError(null)
    try {
      const patchBody = logisticsPatchFromUnitKg(unitKg, line.units_per_box)
      const pmId = line.products_master_id

      if (!pmId && line.variant_id) {
        const created = await createOrderWeightLogistics(line.variant_id, order.document_id)
        const newPmId = Number(created.product?.id)
        if (!newPmId) {
          applyOrderUpdate(created.order)
          return
        }
        const res = await patchOrderWeightProduct(newPmId, order.document_id, patchBody)
        applyOrderUpdate(res.order)
      } else if (pmId) {
        const res = await patchOrderWeightProduct(pmId, order.document_id, patchBody)
        applyOrderUpdate(res.order)
      } else {
        throw new Error("Sin ficha logística para este producto")
      }

      setDraftUnits((prev) => {
        const next = { ...prev }
        delete next[line.detail_id]
        return next
      })
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

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="flex w-full flex-col gap-0 p-0 sm:max-w-2xl">
        <SheetHeader className="border-b border-border/70 px-4 py-4 text-left">
          <SheetTitle className="flex items-center gap-2 text-base">
            <Scale className="size-4 text-primary" aria-hidden />
            {title}
          </SheetTitle>
          <SheetDescription asChild>
            <div className="space-y-2 text-xs text-muted-foreground">
              <p>
                <span className="text-foreground/80">Cliente: </span>
                <span className="font-medium text-foreground">
                  {row?.nombre_fantasia?.trim() || "—"}
                </span>
              </p>
              <div className="flex flex-wrap gap-3 tabular-nums">
                <span>
                  Peso actual:{" "}
                  <strong className="text-foreground">
                    {formatKg(order?.peso_total_kg ?? row?.peso_total_kg ?? row?.weight_kg)}
                  </strong>
                </span>
                <span>
                  Sin peso:{" "}
                  <strong className="text-foreground">
                    {order?.productos_sin_peso ?? row?.productos_sin_peso ?? 0}
                  </strong>
                </span>
                {headerBadge ? (
                  <Badge
                    variant="outline"
                    className={cn("text-[10px]", weightBadgeClass(headerBadge.kind))}
                  >
                    {weightBadgeEmoji(headerBadge.kind)} {headerBadge.label}
                  </Badge>
                ) : null}
              </div>
            </div>
          </SheetDescription>
        </SheetHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3">
          {loading ? (
            <div className="flex items-center justify-center gap-2 py-16 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              Cargando líneas…
            </div>
          ) : error ? (
            <p className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
              {error}
            </p>
          ) : visibleLines.length === 0 ? (
            <p className="py-12 text-center text-sm text-muted-foreground">
              {mode === "incomplete"
                ? "No hay productos pendientes de peso en esta orden."
                : "Sin líneas de producto."}
            </p>
          ) : (
            <div className="overflow-x-auto rounded-md border border-border/70">
              <table className="w-full min-w-[720px] text-left text-xs">
                <thead className="bg-muted/50 text-[9px] uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-2 py-2">Código</th>
                    <th className="px-2 py-2">Producto</th>
                    <th className="px-2 py-2 text-right">Uds</th>
                    <th className="px-2 py-2 text-right">Cajas</th>
                    <th className="px-2 py-2 text-right">P. unit.</th>
                    <th className="px-2 py-2 text-right">P. caja</th>
                    <th className="px-2 py-2">Nuevo unit.</th>
                    <th className="px-2 py-2">Fuente</th>
                    <th className="px-2 py-2 text-right">Acción</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleLines.map((line) => (
                    <WeightLineEditor
                      key={line.detail_id}
                      line={line}
                      draftUnit={draftUnits[line.detail_id] ?? ""}
                      onDraftChange={(v) =>
                        setDraftUnits((prev) => ({ ...prev, [line.detail_id]: v }))
                      }
                      saving={savingLineId === line.detail_id}
                      onSave={() => void saveLine(line)}
                      editable={
                        mode === "incomplete"
                          ? line.estado_linea === "sin_peso"
                          : line.estado_linea === "sin_peso"
                      }
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {mode === "full" && order ? (
            <div className="mt-4 grid grid-cols-2 gap-2 text-xs sm:grid-cols-4">
              <div className="rounded-md border px-2 py-1.5">
                <Label className="text-[10px] text-muted-foreground">Con peso</Label>
                <p className="font-semibold tabular-nums">{order.productos_con_peso}</p>
              </div>
              <div className="rounded-md border px-2 py-1.5">
                <Label className="text-[10px] text-muted-foreground">Sin peso</Label>
                <p className="font-semibold tabular-nums">{order.productos_sin_peso}</p>
              </div>
              <div className="rounded-md border px-2 py-1.5">
                <Label className="text-[10px] text-muted-foreground">Manuales</Label>
                <p className="font-semibold tabular-nums">{order.productos_manuales}</p>
              </div>
              <div className="rounded-md border px-2 py-1.5">
                <Label className="text-[10px] text-muted-foreground">Cobertura</Label>
                <p className="font-semibold tabular-nums">{order.porcentaje_cobertura}%</p>
              </div>
            </div>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}
