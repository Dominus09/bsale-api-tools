"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { validateWarehouseRevision } from "@/lib/warehouse/api"
import { inspectAisleSpacing, newSpacingBlockers, planAisleSpacing, type SpacingInspection, type SpacingPlan } from "@/lib/warehouse/aisleSpacing"
import type { WarehouseElement } from "@/lib/warehouse/types"

type Props = {
  revisionId: number
  sectorId: number
  elements: WarehouseElement[]
  floorLengthM: number
  floorWidthM: number
  onClose: () => void
  onApply: (next: WarehouseElement[], source: WarehouseElement[], revisionId: number) => void
}

const cm = (m: number) => (Math.round(m * 10000) / 100).toLocaleString("es-CL", { maximumFractionDigits: 2 })
const parseCm = (s: string) => s.trim() ? Number(s.trim().replace(",", ".")) / 100 : NaN

function MetricPreview({ inspection, plan }: { inspection: SpacingInspection; plan: SpacingPlan }) {
  const frames = inspection.aisles.map(a => a.element)
  const ids = new Set(frames.map(e => e.physical.id))
  const positions = plan.elements.filter(e => !e.pendingRetire && !e.pendingRemoveGeometry &&
    e.physical.element_type === "position" && ids.has(e.physical.parent_id ?? -1))
  const changed = new Set(plan.changedKeys)
  // Vista horizontal como en el editor a +90°: fondo a la izquierda,
  // entrada a la derecha; circulación bajo los pasillos.
  const box = (e: WarehouseElement) => {
    const g = e.geometry
    return { x: -g.x_m - g.width_m, y: -g.y_m - g.depth_m, width: g.width_m, height: g.depth_m }
  }
  const boxes = [...frames, ...positions].map(box)
  const x0 = Math.min(...boxes.map(b => b.x)) - .4
  const x1 = Math.max(...boxes.map(b => b.x + b.width)) + .4
  const y0 = Math.min(...boxes.map(b => b.y)) - .9
  const y1 = Math.max(...boxes.map(b => b.y + b.height)) + .95
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-2">
      <svg role="img" aria-label="Propuesta métrica de separación entre filas, sin mover los límites de pasillos"
        viewBox={`${x0} ${y0} ${x1-x0} ${y1-y0}`} width="100%" className="block">
        {frames.map(e => {
          const b = box(e)
          return <g key={e.clientKey}>
            <rect {...b} fill="#eef2ff" stroke="#94a3b8" strokeWidth={.045} strokeDasharray=".13 .08" />
            <text x={b.x+b.width/2} y={b.y-.3} fontSize={.32} textAnchor="middle" fontWeight="600" fill="#334155">{e.physical.code}</text>
          </g>
        })}
        {positions.map(e => {
          const b = box(e), moving = changed.has(e.clientKey)
          return <g key={e.clientKey}>
            <title>{e.physical.code}{moving ? " · desplazada" : ""}</title>
            <rect {...b} fill={moving ? (plan.errors.length ? "#fee2e2" : "#bfdbfe") : "#dbeafe"}
              stroke={moving ? (plan.errors.length ? "#dc2626" : "#2563eb") : "#64748b"} strokeWidth={.035} />
          </g>
        })}
        <text x={(x0+x1)/2} y={y1-.28} fontSize={.28} textAnchor="middle" fill="#64748b">CIRCULACIÓN CENTRAL · SIN CAMBIOS DE PROFUNDIDAD</text>
      </svg>
      <div className="flex flex-wrap justify-between gap-2 border-t border-slate-200 pt-2 text-[11px] text-slate-600">
        <span>← Fondo</span>
        <span>Azul: posición propuesta · trazos: límite nominal</span>
        <span>Entrada →</span>
      </div>
    </div>
  )
}

export function AisleSpacingDialog({ revisionId, sectorId, elements, floorLengthM, floorWidthM, onClose, onApply }: Props) {
  const [uniform, setUniform] = useState("20")
  const [values, setValues] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState(false)
  const [serverError, setServerError] = useState<string[]>([])
  const [ack, setAck] = useState(false)
  const request = useRef(0)

  useEffect(() => {
    request.current += 1
    setBusy(false)
    setServerError([])
    setAck(false)
    return () => { request.current += 1 }
  }, [elements, revisionId])

  const result = useMemo(() => {
    try {
      const inspection = inspectAisleSpacing(elements, sectorId)
      const targets = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, parseCm(value)]))
      const plan = planAisleSpacing(elements, sectorId, targets, floorLengthM, floorWidthM)
      return { inspection, plan, error: null }
    } catch (e) {
      return { inspection: null, plan: null, error: e instanceof Error ? e.message : "No se pudo evaluar la distribución." }
    }
  }, [elements, sectorId, values, floorLengthM, floorWidthM])
  const { inspection, plan, error } = result

  const invalidate = () => {
    request.current += 1
    setServerError([])
    setBusy(false)
    setAck(false)
  }
  const change = (key: string, value: string) => {
    invalidate()
    setValues(old => ({ ...old, [key]: value }))
  }
  const setAll = () => {
    if (!inspection || !Number.isFinite(parseCm(uniform)) || parseCm(uniform) < 0) return
    invalidate()
    setValues(Object.fromEntries(inspection.gaps.map(g => [g.key, uniform])))
  }
  const canApply = !!plan && plan.changedKeys.length > 0 && !plan.errors.length &&
    (!plan.circulationWarnings.length || ack) && !busy

  const verifyAndApply = async () => {
    if (!plan || !canApply) return
    const token = ++request.current
    setBusy(true)
    setServerError([])
    try {
      // Ambos llamados son de sólo lectura. Comparar borrador local actual
      // con la propuesta, incluidos cambios previos aún no guardados.
      const [before, after] = await Promise.all([
        validateWarehouseRevision(revisionId, elements),
        validateWarehouseRevision(revisionId, plan.elements),
      ])
      if (request.current !== token) return
      if (!Array.isArray(before.issues) || !Array.isArray(after.issues)) {
        setServerError(["No se recibió una validación completa del servidor."])
        return
      }
      const blockers = newSpacingBlockers(before.issues, after.issues)
      if (blockers.length) {
        setServerError(blockers.map(i => String(i.message || i.code || "Conflicto nuevo")))
        return
      }
      onApply(plan.elements, elements, revisionId)
    } catch (e) {
      if (request.current === token) setServerError([e instanceof Error ? e.message : "No se pudo validar con la API."])
    } finally {
      if (request.current === token) setBusy(false)
    }
  }

  return (
    <Dialog open onOpenChange={v => { if (!v) { request.current += 1; onClose() } }}>
      <DialogContent className="flex max-h-[94vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1040px]">
        <DialogHeader className="shrink-0 border-b px-5 py-4">
          <DialogTitle className="text-xl">Separación entre pasillos (B)</DialogTitle>
          <p className="text-sm text-slate-500">Ajustar la distancia libre entre filas de pallets de pasillos vecinos.</p>
        </DialogHeader>
        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto bg-slate-50 p-4">
          <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-950">
            Se mantienen los límites físicos de cada pasillo, oficina, escalera, circulación y códigos existentes.
            Al aumentar B se reduce la separación interior A. No cambia la profundidad ni autoriza almacenamiento, circulación o apilamiento.
          </div>
          {error ? <p className="rounded-md bg-red-50 p-3 text-sm text-red-800">{error}</p> : null}
          {inspection && plan ? (
            <div className="grid min-w-0 gap-4 md:grid-cols-[minmax(270px,.8fr)_minmax(0,1.2fr)]">
              <section className="space-y-3 rounded-xl border bg-white p-4">
                <h3 className="text-sm font-semibold">1. Separaciones B (cm)</h3>
                <div className="flex items-end gap-2 rounded-lg border bg-slate-50 p-3">
                  <label className="min-w-0 flex-1 space-y-1 text-xs font-medium">
                    <span>Aplicar a todos</span>
                    <Input inputMode="decimal" value={uniform} disabled={busy} onChange={e => setUniform(e.target.value)} className="h-9 bg-white" />
                  </label>
                  <Button type="button" variant="outline" size="sm" disabled={busy || !Number.isFinite(parseCm(uniform)) || parseCm(uniform) < 0} onClick={setAll}>Usar medida</Button>
                </div>
                <div className="grid grid-cols-[1fr_68px_90px] items-center gap-2 text-xs">
                  <span className="text-slate-500">Par</span><span className="text-right text-slate-500">Actual</span><span className="text-slate-500">Deseada</span>
                  {inspection.gaps.map(g => (
                    <div key={g.key} className="contents">
                      <label htmlFor={"gap-"+g.key} className="font-medium">{g.left} ↔ {g.right}</label>
                      <span className="text-right tabular-nums text-slate-600">{cm(g.currentM)}</span>
                      <Input id={"gap-"+g.key} inputMode="decimal" disabled={busy}
                        value={values[g.key] ?? String(Math.round(g.currentM*10000)/100)}
                        onChange={e => change(g.key,e.target.value)} className="h-9 tabular-nums" />
                    </div>
                  ))}
                </div>
                <Button type="button" size="sm" variant="ghost" disabled={busy} onClick={() => { invalidate(); setValues({}) }}>Restaurar valores actuales</Button>
                <p className="text-xs text-slate-500">Las medidas se calculan entre las huellas existentes, no entre las líneas de los marcos nominales.</p>
              </section>
              <section className="min-w-0 space-y-3 rounded-xl border bg-white p-4">
                <h3 className="text-sm font-semibold">2. Vista previa métrica</h3>
                <MetricPreview inspection={inspection} plan={plan}/>
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-lg border bg-slate-50 p-2">
                    <p className="text-xs text-slate-500">Posiciones que cambian</p>
                    <p className="text-xl font-semibold tabular-nums">{plan.changedKeys.length}</p>
                  </div>
                  <div className="rounded-lg border bg-slate-50 p-2">
                    <p className="text-xs text-slate-500">Menor separación interior A</p>
                    <p className="text-xl font-semibold tabular-nums">{cm(Math.min(...plan.interior.map(a => a.proposedM)))} cm</p>
                  </div>
                </div>
                <h4 className="text-xs font-semibold text-slate-700">Separación interior A (cm)</h4>
                <div className="grid grid-cols-[1fr_1fr_1fr] gap-x-3 gap-y-1 text-xs">
                  <span className="text-slate-500">Pasillo</span><span className="text-right text-slate-500">Actual</span><span className="text-right text-slate-500">Propuesta</span>
                  {plan.interior.map(a => (
                    <div key={a.code} className="contents">
                      <span className="font-medium">{a.code}</span>
                      <span className="text-right tabular-nums">{cm(a.currentM)}</span>
                      <span className={`text-right tabular-nums font-medium ${a.proposedM < 0 ? "text-red-700" : ""}`}>{cm(a.proposedM)}</span>
                    </div>
                  ))}
                </div>
                <details className="text-xs text-slate-600">
                  <summary className="cursor-pointer font-medium">Ver separación B efectiva propuesta</summary>
                  <div className="mt-2 space-y-1">{plan.gaps.map(g => <div key={g.key} className="flex justify-between"><span>{g.left} ↔ {g.right}</span><strong>{cm(g.proposedM)} cm</strong></div>)}</div>
                </details>
              </section>
            </div>
          ) : null}
          {plan?.errors.length || serverError.length ? (
            <div className="space-y-1 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-900">
              <div className="flex items-center gap-2 font-semibold"><AlertTriangle className="h-4 w-4"/>Conflictos que impiden aplicar</div>
              <ul className="max-h-40 space-y-1 overflow-y-auto">{[...(plan?.errors ?? []), ...serverError].map((message,i) => <li key={i}>{message}</li>)}</ul>
            </div>
          ) : null}
          {plan?.circulationWarnings.length ? (
            <div className="space-y-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-950">
              <p>Persisten extensiones observadas hacia circulación en {plan.circulationWarnings.length} casos. Este ajuste no las elimina ni las autoriza.</p>
              <details><summary className="cursor-pointer">Ver posiciones</summary><ul className="mt-1 max-h-28 overflow-y-auto">{plan.circulationWarnings.map(w => <li key={w}>{w}</li>)}</ul></details>
              <label className="flex items-start gap-2"><input type="checkbox" className="mt-0.5" checked={ack} disabled={busy} onChange={e => setAck(e.target.checked)}/>Entiendo que se documenta un borrador pendiente de validación física.</label>
            </div>
          ) : null}
        </div>
        <DialogFooter className="shrink-0 border-t px-5 py-3">
          <Button type="button" variant="outline" onClick={onClose}>Cancelar</Button>
          <Button type="button" disabled={!canApply} onClick={() => void verifyAndApply()}>
            {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin"/> : null}
            Validar e incorporar al borrador
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
