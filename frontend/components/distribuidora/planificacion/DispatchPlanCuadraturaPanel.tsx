"use client"

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react"
import { Loader2, Plus, Save, Trash2 } from "lucide-react"

import {
  getDispatchPlanCuadratura,
  putDispatchPlanCuadratura,
  type DispatchPlanCuadraturaResponse,
} from "@/lib/api"
import {
  computeCuadraturaResult,
  diffStatusClass,
  emptyCreditNoteRow,
  emptyNotLoadedRow,
  observacionRequired,
  type CuadraturaCreditNoteRow,
  type CuadraturaNotLoadedRow,
} from "@/lib/dispatch-plan-cuadratura"
import { formatClp } from "@/lib/ors-map-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"

type DispatchPlanCuadraturaPanelProps = {
  planId: number
  onMessage: (msg: string) => void
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

export function DispatchPlanCuadraturaPanel({ planId, onMessage }: DispatchPlanCuadraturaPanelProps) {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [ventaOc, setVentaOc] = useState(0)
  const [ventaFacturada, setVentaFacturada] = useState(0)
  const [ventaPicking, setVentaPicking] = useState(0)
  const [transferencia, setTransferencia] = useState(0)
  const [efectivo, setEfectivo] = useState(0)
  const [cheque, setCheque] = useState(0)
  const [debito, setDebito] = useState(0)
  const [observacion, setObservacion] = useState("")
  const [creditNotes, setCreditNotes] = useState<CuadraturaCreditNoteRow[]>([])
  const [notLoaded, setNotLoaded] = useState<CuadraturaNotLoadedRow[]>([])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data: DispatchPlanCuadraturaResponse = await getDispatchPlanCuadratura(planId)
      setVentaOc(data.ventas.venta_oc_clp)
      setVentaFacturada(data.ventas.venta_facturada_clp)
      setVentaPicking(data.ventas.venta_picking_clp)
      setTransferencia(data.pagos.transferencia_clp)
      setEfectivo(data.pagos.efectivo_clp)
      setCheque(data.pagos.cheque_clp)
      setDebito(data.pagos.debito_clp)
      setObservacion(data.observacion || "")
      setCreditNotes(data.credit_notes?.length ? data.credit_notes : [])
      setNotLoaded(data.not_loaded?.length ? data.not_loaded : [])
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
      computeCuadraturaResult({
        venta_picking_clp: ventaPicking,
        credit_notes: creditNotes,
        not_loaded: notLoaded,
        transferencia_clp: transferencia,
        efectivo_clp: efectivo,
        cheque_clp: cheque,
        debito_clp: debito,
      }),
    [ventaPicking, creditNotes, notLoaded, transferencia, efectivo, cheque, debito],
  )

  const obsRequired = observacionRequired(resultado.diferencia_clp)

  const save = async () => {
    if (obsRequired && !observacion.trim()) {
      onMessage("Debe ingresar una observación cuando la diferencia es distinta de cero.")
      return
    }
    setSaving(true)
    try {
      await putDispatchPlanCuadratura(planId, {
        transferencia_clp: transferencia,
        efectivo_clp: efectivo,
        cheque_clp: cheque,
        debito_clp: debito,
        observacion: observacion.trim() || null,
        credit_notes: creditNotes,
        not_loaded: notLoaded,
      })
      onMessage("Cuadratura guardada.")
      await load()
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al guardar cuadratura")
    } finally {
      setSaving(false)
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

  return (
    <div className="space-y-6">
      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Ventas</h3>
        <div className="grid gap-2 sm:grid-cols-3">
          <ReadOnlyAmount label="Venta OC" value={ventaOc} />
          <ReadOnlyAmount label="Venta facturada" value={ventaFacturada} />
          <ReadOnlyAmount label="Venta picking" value={ventaPicking} />
        </div>
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Medios de pago</h3>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {(
            [
              ["Transferencia", transferencia, setTransferencia],
              ["Efectivo", efectivo, setEfectivo],
              ["Cheque", cheque, setCheque],
              ["Débito", debito, setDebito],
            ] as const
          ).map(([label, value, setter]) => (
            <div key={label} className="space-y-1">
              <Label className="text-xs">{label}</Label>
              <Input
                inputMode="numeric"
                value={value ? String(value) : ""}
                placeholder="0"
                onChange={(e) => setter(parseClpInput(e.target.value))}
              />
            </div>
          ))}
        </div>
      </section>

      <EditableTableSection
        title="Notas de crédito"
        columns={["Documento venta", "Nota crédito", "Monto", "Motivo"]}
        rows={creditNotes}
        onChange={setCreditNotes}
        onAdd={() => setCreditNotes((r) => [...r, emptyCreditNoteRow()])}
        renderRow={(row, idx, update) => (
          <>
            <Input
              value={row.documento_venta}
              onChange={(e) => update(idx, { ...row, documento_venta: e.target.value })}
              placeholder="Doc. venta"
            />
            <Input
              value={row.nota_credito}
              onChange={(e) => update(idx, { ...row, nota_credito: e.target.value })}
              placeholder="NC"
            />
            <Input
              inputMode="numeric"
              value={row.monto ? String(row.monto) : ""}
              onChange={(e) => update(idx, { ...row, monto: parseClpInput(e.target.value) })}
              placeholder="0"
            />
            <Input
              value={row.motivo}
              onChange={(e) => update(idx, { ...row, motivo: e.target.value })}
              placeholder="Motivo"
            />
          </>
        )}
      />

      <EditableTableSection
        title="No cargados"
        columns={["Cliente", "Documento", "Monto", "Motivo"]}
        rows={notLoaded}
        onChange={setNotLoaded}
        onAdd={() => setNotLoaded((r) => [...r, emptyNotLoadedRow()])}
        renderRow={(row, idx, update) => (
          <>
            <Input
              value={row.cliente}
              onChange={(e) => update(idx, { ...row, cliente: e.target.value })}
              placeholder="Cliente"
            />
            <Input
              value={row.documento}
              onChange={(e) => update(idx, { ...row, documento: e.target.value })}
              placeholder="Documento"
            />
            <Input
              inputMode="numeric"
              value={row.monto ? String(row.monto) : ""}
              onChange={(e) => update(idx, { ...row, monto: parseClpInput(e.target.value) })}
              placeholder="0"
            />
            <Input
              value={row.motivo}
              onChange={(e) => update(idx, { ...row, motivo: e.target.value })}
              placeholder="Motivo"
            />
          </>
        )}
      />

      <section className="space-y-2">
        <h3 className="text-sm font-semibold">Resultado</h3>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          <ReadOnlyAmount label="Venta picking" value={ventaPicking} />
          <ReadOnlyAmount label="(−) Notas crédito" value={-resultado.notas_credito_clp} />
          <ReadOnlyAmount label="(−) No cargados" value={-resultado.no_cargados_clp} />
          <ReadOnlyAmount label="Venta ajustada" value={resultado.venta_ajustada_clp} />
          <ReadOnlyAmount label="Total recaudado" value={resultado.total_recaudado_clp} />
          <div
            className={cn(
              "rounded-md border p-3",
              diffStatusClass(resultado.diferencia_status),
            )}
          >
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
          value={observacion}
          onChange={(e) => setObservacion(e.target.value)}
          placeholder={
            obsRequired
              ? "Obligatorio: explique la diferencia operacional"
              : "Opcional"
          }
          rows={3}
        />
      </section>

      <div className="flex flex-wrap gap-2 border-t pt-4">
        <Button type="button" onClick={() => void save()} disabled={saving}>
          {saving ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Save className="mr-2 size-4" />}
          Guardar cuadratura
        </Button>
        <Button type="button" variant="outline" disabled={saving} onClick={() => void load()}>
          Recargar
        </Button>
      </div>
    </div>
  )
}

function EditableTableSection<T>({
  title,
  columns,
  rows,
  onChange,
  onAdd,
  renderRow,
}: {
  title: string
  columns: string[]
  rows: T[]
  onChange: (rows: T[]) => void
  onAdd: () => void
  renderRow: (
    row: T,
    idx: number,
    update: (idx: number, next: T) => void,
  ) => ReactNode
}) {
  const update = (idx: number, next: T) => {
    onChange(rows.map((r, i) => (i === idx ? next : r)))
  }
  const remove = (idx: number) => {
    onChange(rows.filter((_, i) => i !== idx))
  }

  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        <Button type="button" variant="outline" size="sm" onClick={onAdd}>
          <Plus className="mr-1 size-3.5" />
          Agregar fila
        </Button>
      </div>
      {rows.length === 0 ? (
        <p className="text-xs text-muted-foreground">Sin registros. Use «Agregar fila» si aplica.</p>
      ) : (
        <div className="space-y-2">
          <div className="hidden gap-2 text-[10px] font-medium uppercase tracking-wide text-muted-foreground sm:grid sm:grid-cols-[1fr_1fr_120px_1fr_36px]">
            {columns.map((c) => (
              <span key={c}>{c}</span>
            ))}
            <span />
          </div>
          {rows.map((row, idx) => (
            <div
              key={idx}
              className="grid gap-2 sm:grid-cols-[1fr_1fr_120px_1fr_36px] sm:items-center"
            >
              {renderRow(row, idx, update)}
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-8 shrink-0 text-muted-foreground hover:text-destructive"
                onClick={() => remove(idx)}
                aria-label="Eliminar fila"
              >
                <Trash2 className="size-4" />
              </Button>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}
