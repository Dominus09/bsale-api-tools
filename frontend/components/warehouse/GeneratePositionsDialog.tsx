"use client"

import { useEffect, useRef, useState } from "react"
import { AlertTriangle, CheckCircle2, Loader2 } from "lucide-react"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { previewGeneratePositions, type GeneratePreviewResult, type GeneratePositionIssue } from "@/lib/warehouse/api"
import { GENERATOR_PRESETS, STANDARD_PALLET_M } from "@/lib/warehouse/style"
import type { WarehouseElement } from "@/lib/warehouse/types"
import { GeneratorPlanPreview } from "@/components/warehouse/GeneratorPlanPreview"

type Props = {
  open: boolean
  onOpenChange: (open: boolean) => void
  parent: WarehouseElement | null
  floorLengthM: number
  floorWidthM: number
  existing: WarehouseElement[]
  onConfirm: (preview: GeneratePreviewResult) => void
}

type FormState = {
  axis: "x" | "y"
  preset: string
  rows: string
  orientation: "standard" | "rotated"
  rowOrientations: string
  palletL: string
  palletW: string
  gapLong: string
  gapTrans: string
  autoGap: boolean
  marginStart: string
  marginEnd: string
  marginNear: string
  marginFar: string
  anchor: "start" | "end"
  grow: "near_to_far" | "far_to_near"
  prefix: string
  startIndex: string
  height: string
  allowOutside: boolean
}

function initialForm(parent: WarehouseElement | null, axis?: "x" | "y"): FormState {
  const longitudinal = axis ? axis === "x" : parent?.physical.code === "IZ" ||
    (parent?.physical.element_type === "sector" && parent.geometry.width_m > parent.geometry.depth_m)
  const iz = longitudinal && parent?.physical.code === "IZ"
  return {
    axis: longitudinal ? "x" : "y",
    preset: longitudinal ? "custom" : "4+4",
    rows: longitudinal ? "10,10" : "4,4",
    orientation: "standard",
    rowOrientations: "",
    palletL: String(STANDARD_PALLET_M.length),
    palletW: String(STANDARD_PALLET_M.width),
    gapLong: longitudinal ? "0.05" : "0",
    gapTrans: "0",
    autoGap: true,
    marginStart: iz ? "0.05" : "0",
    marginEnd: iz ? "1.26" : "0",
    marginNear: iz ? "0.05" : "0",
    marginFar: iz ? "0.05" : "0",
    anchor: longitudinal ? "start" : "end",
    grow: longitudinal ? "far_to_near" : "near_to_far",
    prefix: parent?.physical.code || "",
    startIndex: "1",
    height: parent?.geometry.height_clear_m != null ? String(parent.geometry.height_clear_m) : "",
    allowOutside: false,
  }
}

function decimal(s: string): number {
  if (!s.trim()) return NaN
  return Number(s.trim().replace(",", "."))
}

function fmt(value: number, digits = 2) {
  return value.toLocaleString("es-CL", { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function metricRequest(form: FormState, parent: WarehouseElement, existing: WarehouseElement[], floorLengthM: number, floorWidthM: number) {
  const rowCounts = form.rows.split(/[,\s+]+/).filter(Boolean).map(Number)
  const rowOrientations = form.rowOrientations.split(/[,\s]+/).filter(Boolean)
  const paired = parent.physical.code === "IZ" && form.axis === "x"
  if (paired && !(Number(form.rows.split(",")[0]) > 0)) {
    throw new Error("Indique al menos la cantidad de la fila delantera D; puede dejar A vacía.")
  }
  if (!rowCounts.length || rowCounts.some(n => !Number.isInteger(n) || n <= 0) ||
      rowCounts.length > 50 || rowCounts.reduce((s, n) => s + n, 0) > 500 ||
      (paired && rowCounts.length > 2)) {
    throw new Error("Indique conteos enteros positivos (máximo 500 posiciones; IZ admite hasta dos filas).")
  }
  if (rowOrientations.length > rowCounts.length || rowOrientations.some(o => o !== "standard" && o !== "rotated")) {
    throw new Error("Orientaciones por fila inválidas.")
  }
  const palletL = decimal(form.palletL)
  const palletW = decimal(form.palletW)
  const gapLong = decimal(form.gapLong)
  const gapTrans = form.autoGap ? 0 : decimal(form.gapTrans)
  const margins = [form.marginStart, form.marginEnd, form.marginNear, form.marginFar].map(decimal)
  const start = decimal(form.startIndex)
  const height = form.height.trim() ? decimal(form.height) : null
  if (![palletL, palletW].every(v => Number.isFinite(v) && v > 0)) throw new Error("Dimensiones de pallet inválidas.")
  if (![gapLong, gapTrans, ...margins].every(v => Number.isFinite(v) && v >= 0)) throw new Error("Separaciones y márgenes deben ser números no negativos.")
  if (!Number.isInteger(start) || start < 1 || !form.prefix.trim()) throw new Error("Prefijo y numeración inválidos.")
  if (height !== null && (!Number.isFinite(height) || height <= 0)) throw new Error("Altura libre inválida.")
  if (parent.physical.id <= 0 || parent.pendingRemoveGeometry || parent.pendingRetire) {
    throw new Error("Guarde primero el sector o pasillo.")
  }
  if (existing.some(e => e.physical.parent_id === parent.physical.id && e.physical.element_type === "aisle" && !e.pendingRetire && !e.pendingRemoveGeometry)) {
    throw new Error("Este sector contiene pasillos. Seleccione un pasillo individual.")
  }
  return {
    parent: {
      x_m: parent.geometry.x_m, y_m: parent.geometry.y_m,
      width_m: parent.geometry.width_m, depth_m: parent.geometry.depth_m,
      rotation_deg: parent.geometry.rotation_deg,
      code: parent.physical.code, element_type: parent.physical.element_type,
    },
    parent_physical_id: parent.physical.id,
    row_axis: form.axis,
    auto_row_gap: form.autoGap,
    pallet_length_m: palletL, pallet_width_m: palletW,
    preset: form.preset,
    row_counts: rowCounts,
    row_orientations: paired ? [] : rowOrientations,
    default_orientation: form.orientation,
    gap_longitudinal_m: gapLong, gap_transverse_m: gapTrans,
    margin_start_m: margins[0], margin_end_m: margins[1],
    margin_near_m: margins[2], margin_far_m: margins[3],
    anchor: form.anchor, grow_rows: paired ? "far_to_near" : form.grow,
    code_prefix: form.prefix.trim(), code_suffix_mode: paired ? "iz_da" : "numeric",
    start_index: start, height_clear_m: height,
    floor_length_m: floorLengthM, floor_width_m: floorWidthM,
    existing_elements: existing.filter(e => !e.pendingRetire && !e.pendingRemoveGeometry)
      .map(e => ({ physical: e.physical, geometry: e.geometry })),
    // existing_elements contiene las estructuras. No duplicarlas como obstacles.
    allow_outside_parent: !paired && form.allowOutside,
  }
}

function Field({ label, value, onChange, integer = false }: {
  label: string
  value: string
  onChange: (v: string) => void
  integer?: boolean
}) {
  return (
    <label className="block min-w-0 space-y-1">
      <span className="block text-xs font-medium text-slate-600">{label}</span>
      <Input value={value} onChange={e => onChange(e.target.value)} inputMode={integer ? "numeric" : "decimal"} className="h-9 bg-white" />
    </label>
  )
}

function IssueList({ title, issues, danger }: { title: string; issues: GeneratePositionIssue[]; danger?: boolean }) {
  if (!issues.length) return null
  return (
    <div className={`rounded-lg border p-3 text-xs ${danger ? "border-red-200 bg-red-50 text-red-800" : "border-amber-200 bg-amber-50 text-amber-900"}`}>
      <div className="mb-1 flex items-center gap-2 font-semibold"><AlertTriangle className="h-4 w-4" />{title} ({issues.length})</div>
      <ul className="max-h-40 space-y-1 overflow-y-auto">
        {issues.map((issue, i) => (
          <li key={`${issue.code}-${i}`}>{issue.generated_codes?.length ? <strong>{issue.generated_codes.join(", ")} · </strong> : null}{issue.message}</li>
        ))}
      </ul>
    </div>
  )
}

export function GeneratePositionsDialog({ open, onOpenChange, parent, floorLengthM, floorWidthM, existing, onConfirm }: Props) {
  const [form, setForm] = useState<FormState>(() => initialForm(parent))
  const [result, setResult] = useState<{ fingerprint: string; data: GeneratePreviewResult } | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [ack, setAck] = useState(false)
  const [large, setLarge] = useState(false)
  const sequence = useRef(0)

  useEffect(() => {
    sequence.current += 1
    setLoading(false)
    setResult(null)
    setError(null)
    setAck(false)
    setLarge(false)
    if (open) setForm(initialForm(parent))
  }, [open, parent])

  const patch = <K extends keyof FormState,>(key: K, value: FormState[K]) => {
    setForm(old => ({ ...old, [key]: value }))
    setAck(false)
    setError(null)
  }
  const paired = form.axis === "x" && parent?.physical.code === "IZ"
  const rows = form.rows.split(",")
  let request: ReturnType<typeof metricRequest> | null = null
  let configError: string | null = null
  if (parent) {
    try {
      request = metricRequest(form, parent, existing, floorLengthM, floorWidthM)
    } catch (e) {
      configError = e instanceof Error ? e.message : "Configuración inválida"
    }
  }
  const fingerprint = request ? JSON.stringify(request) : ""
  const preview = result && fingerprint === result.fingerprint ? result.data : null
  const outdated = !!result && !preview
  const blockers = preview?.blocking_issues ?? []
  const warnings = preview?.position_issues?.filter(i => i.severity === "warning") ?? []
  const canConfirm = !loading && preview?.can_incorporate === true &&
    preview.row_axis === form.axis && (!preview.requires_extension_ack || ack)

  const runPreview = async () => {
    if (!request) return
    const token = ++sequence.current
    const sentFingerprint = fingerprint
    setLoading(true)
    setError(null)
    setResult(null)
    setAck(false)
    try {
      const response = await previewGeneratePositions(request)
      if (sequence.current !== token) return
      if (response.row_axis !== request.row_axis || typeof response.can_incorporate !== "boolean") {
        setError("La API no tiene la versión compatible del generador. Actualice primero el backend.")
        return
      }
      setResult({ fingerprint: sentFingerprint, data: response })
    } catch (e) {
      if (sequence.current === token) setError(e instanceof Error ? e.message : "Error en vista previa")
    } finally {
      if (sequence.current === token) setLoading(false)
    }
  }

  const close = (value: boolean) => {
    if (!value) {
      sequence.current += 1
      setLoading(false)
      setResult(null)
    }
    onOpenChange(value)
  }

  const parentLabel = parent ? `${parent.physical.code} · ${parent.physical.name}` : ""
  const along = form.axis === "x" ? "X" : "Y"
  const across = form.axis === "x" ? "Y" : "X"
  const setPair = (index: number, value: string) => {
    const next = [rows[0] ?? "", rows[1] ?? ""]
    next[index] = value
    patch("rows", next.join(","))
    patch("preset", "custom")
  }

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="flex max-h-[94vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1080px]">
        <DialogHeader className="shrink-0 border-b bg-white px-5 py-4">
          <DialogTitle className="text-xl">Generar posiciones de almacenamiento</DialogTitle>
          {parent ? <p className="text-sm text-slate-500">{parentLabel} · {fmt(parent.geometry.width_m)} × {fmt(parent.geometry.depth_m)} m</p> : null}
        </DialogHeader>
        {!parent ? <p className="p-5 text-sm text-amber-800">Seleccione un sector o pasillo.</p> : (
          <div className="min-h-0 flex-1 overflow-y-auto bg-slate-50 p-4">
            <div className="grid min-w-0 gap-4 md:grid-cols-[minmax(280px,0.9fr)_minmax(0,1.1fr)]">
              <div className="space-y-3">
                <section className="space-y-3 rounded-xl border bg-white p-4">
                  <div className="text-sm font-semibold text-slate-900">1. Distribución</div>
                  <div className="grid grid-cols-2 gap-2">
                    {(["x", "y"] as const).map(axis => (
                      <button type="button" key={axis} onClick={() => {
                        if (axis !== form.axis) {
                          setForm(initialForm(parent, axis))
                          setAck(false)
                          setError(null)
                        }
                      }} className={`rounded-lg border p-3 text-left text-xs ${axis === form.axis ? "border-red-700 bg-red-50 text-red-900 ring-1 ring-red-700" : "border-slate-200 bg-white text-slate-600"}`}>
                        <span className="block font-semibold">{axis === "x" ? "Longitudinal" : "Transversal"}</span>
                        <span className="mt-1 block">{axis === "x" ? "Entrada → fondo (X)" : "Circulación → muro (Y)"}</span>
                      </button>
                    ))}
                  </div>
                  {paired ? (
                    <>
                      <div className="grid grid-cols-2 gap-2">
                        <Field label="Fila delantera (D)" value={rows[0] ?? ""} onChange={v => setPair(0, v)} integer />
                        <Field label="Fila trasera (A, opcional)" value={rows[1] ?? ""} onChange={v => setPair(1, v)} integer />
                      </div>
                      <p className="text-xs text-slate-500">D junto a circulación; A junto al muro izquierdo. Numeración emparejada desde la entrada. IZ-SU se conserva como zona de sueltos.</p>
                    </>
                  ) : (
                    <>
                      <Label className="text-xs">Configuración de filas</Label>
                      <Select value={form.preset} onValueChange={v => {
                        patch("preset", v)
                        if (v !== "custom") patch("rows", v.replace("+", ","))
                      }}>
                        <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                        <SelectContent>{GENERATOR_PRESETS.map(p => <SelectItem key={p} value={p}>{p === "custom" ? "Personalizada" : p}</SelectItem>)}</SelectContent>
                      </Select>
                      <Field label="Pallets por fila (ej. 4,5)" value={form.rows} onChange={v => { patch("rows", v); patch("preset", "custom") }} />
                    </>
                  )}
                  <Label className="text-xs">Orientación de pallets</Label>
                  <Select value={form.orientation} onValueChange={v => patch("orientation", v === "rotated" ? "rotated" : "standard")}>
                    <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="standard">Lado largo sobre {along}</SelectItem>
                      <SelectItem value="rotated">Lado corto sobre {along}</SelectItem>
                    </SelectContent>
                  </Select>
                  <div className="grid grid-cols-2 gap-2">
                    <Field label="Pallet largo (m)" value={form.palletL} onChange={v => patch("palletL", v)} />
                    <Field label="Pallet ancho (m)" value={form.palletW} onChange={v => patch("palletW", v)} />
                  </div>
                  <Field label="Separación entre pallets de una fila (m)" value={form.gapLong} onChange={v => patch("gapLong", v)} />
                  <label className="flex items-start gap-2 text-xs text-slate-700">
                    <input className="mt-0.5 accent-red-700" type="checkbox" checked={form.autoGap} onChange={e => patch("autoGap", e.target.checked)} />
                    <span>Distribuir filas en el ancho disponible (respetando márgenes)</span>
                  </label>
                  {!form.autoGap ? <Field label="Separación entre filas (m)" value={form.gapTrans} onChange={v => patch("gapTrans", v)} /> : null}
                </section>

                <details className="rounded-xl border bg-white p-4">
                  <summary className="cursor-pointer text-sm font-semibold">2. Configuración avanzada</summary>
                  <div className="mt-3 space-y-3">
                    <div className="grid grid-cols-2 gap-2">
                      <Field label={form.axis === "x" ? "Margen entrada (X bajo)" : "Margen circulación (Y bajo)"} value={form.marginStart} onChange={v => patch("marginStart", v)} />
                      <Field label={form.axis === "x" ? "Margen fondo (X alto)" : "Margen muro (Y alto)"} value={form.marginEnd} onChange={v => patch("marginEnd", v)} />
                      <Field label={`Margen ${across} bajo`} value={form.marginNear} onChange={v => patch("marginNear", v)} />
                      <Field label={`Margen ${across} alto`} value={form.marginFar} onChange={v => patch("marginFar", v)} />
                    </div>
                    <Label className="text-xs">Anclar filas</Label>
                    <Select value={form.anchor} onValueChange={v => patch("anchor", v === "end" ? "end" : "start")}>
                      <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                      <SelectContent><SelectItem value="start">{form.axis === "x" ? "Entrada" : "Circulación"}</SelectItem><SelectItem value="end">{form.axis === "x" ? "Fondo" : "Muro exterior"}</SelectItem></SelectContent>
                    </Select>
                    {!paired ? (
                      <>
                        <Label className="text-xs">Orden transversal de filas</Label>
                        <Select value={form.grow} onValueChange={v => patch("grow", v === "far_to_near" ? "far_to_near" : "near_to_far")}>
                          <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                          <SelectContent><SelectItem value="near_to_far">{across} bajo → alto</SelectItem><SelectItem value="far_to_near">{across} alto → bajo</SelectItem></SelectContent>
                        </Select>
                        <Field label="Orientaciones por fila (standard,rotated)" value={form.rowOrientations} onChange={v => patch("rowOrientations", v)} />
                      </>
                    ) : null}
                    <div className="grid grid-cols-2 gap-2">
                      <Field label="Prefijo de código" value={form.prefix} onChange={v => patch("prefix", v)} />
                      <Field label="Inicio numeración" value={form.startIndex} onChange={v => patch("startIndex", v)} integer />
                    </div>
                    <Field label="Altura libre de referencia (m, opcional)" value={form.height} onChange={v => patch("height", v)} />
                    {!paired ? <label className="flex items-start gap-2 text-xs text-amber-900"><input type="checkbox" className="mt-0.5 accent-red-700" checked={form.allowOutside} onChange={e => patch("allowOutside", e.target.checked)} /><span>Permitir evaluar una extensión fuera del sector como propuesta pendiente (no autoriza ocupar circulación).</span></label> : null}
                  </div>
                </details>
                {paired ? <p className="px-1 text-xs text-slate-500">Propuesta inicial: 10 + 10, margen posterior de 1,26 m. Ajustar a la distribución física real; no es capacidad aprobada.</p> : null}
              </div>

              <section className="min-w-0 space-y-3 rounded-xl border bg-white p-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div><h3 className="text-sm font-semibold">3. Vista previa del plano</h3><p className="text-xs text-slate-500">Coordenadas reales de la propuesta, sin guardar</p></div>
                  <Button type="button" size="sm" disabled={loading || !request} onClick={() => void runPreview()}>
                    {loading ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" /> : null} Vista previa
                  </Button>
                </div>
                {configError || error ? <p className="rounded-md bg-red-50 p-2 text-xs text-red-800">{configError || error}</p> : null}
                {outdated ? <p className="rounded-md bg-amber-50 p-2 text-xs text-amber-900">Cambiaste los parámetros. Genera una nueva vista previa antes de incorporar.</p> : null}
                <GeneratorPlanPreview parent={parent} existing={existing} preview={preview} large={large} />
                <div className="flex items-center justify-between text-[11px] text-slate-500">
                  <span>Fondo arriba · entrada abajo</span>
                  <Button size="sm" type="button" variant="ghost" onClick={() => setLarge(v => !v)}>{large ? "Ver completo" : "Ampliar"}</Button>
                </div>
                {preview ? (
                  <>
                    <div className="grid grid-cols-2 gap-2">
                      {[
                        ["Posiciones propuestas", String(preview.position_count)],
                        ["Superficie ocupada", `${fmt(preview.occupied_area_m2)} m²`],
                        ["Separación entre filas", preview.interior_gap_m == null ? "—" : `${fmt(preview.interior_gap_m)} m`],
                        ["Fuera del límite", `${fmt(preview.extension_outside_parent_m)} m`],
                      ].map(([title, value]) => <div className="rounded-lg border bg-slate-50 p-2" key={title}><p className="text-[11px] text-slate-500">{title}</p><p className="mt-0.5 text-lg font-semibold tabular-nums text-slate-900">{value}</p></div>)}
                    </div>
                    <IssueList title="Conflictos de esta propuesta" issues={blockers} danger />
                    <IssueList title="Advertencias de esta propuesta" issues={warnings} />
                    {preview.warnings.map((w, i) => <p key={i} className="text-xs text-amber-900">{w.message}</p>)}
                    {preview.can_incorporate === true && !preview.requires_extension_ack ? <div className="flex items-center gap-2 rounded-md bg-emerald-50 p-2 text-xs text-emerald-800"><CheckCircle2 className="h-4 w-4 shrink-0" />Sin conflictos geométricos bloqueantes de las posiciones propuestas. Validación física pendiente.</div> : null}
                    {preview.requires_extension_ack && preview.can_incorporate === true ? <label className="flex items-start gap-2 rounded-md bg-amber-50 p-2 text-xs text-amber-900"><input type="checkbox" className="mt-0.5" checked={ack} onChange={e => setAck(e.target.checked)} />Entiendo que esta extensión es solo una propuesta; no autoriza ocupar circulación ni almacenar.</label> : null}
                    <details className="text-xs text-slate-600"><summary className="cursor-pointer font-medium">Ver códigos y coordenadas ({preview.position_count})</summary><div className="mt-2 max-h-40 overflow-y-auto font-mono text-[11px]">{preview.positions.map(p => <div key={p.code}>{p.code}: X {fmt(Number(p.geometry.x_m))} · Y {fmt(Number(p.geometry.y_m))}</div>)}</div></details>
                  </>
                ) : <p className="text-xs text-slate-500">Configura las filas y presiona Vista previa. Las posiciones candidatas aparecerán aquí.</p>}
                <p className="border-t pt-2 text-xs text-slate-500">No representa inventario ni capacidad segura autorizada. Todas las posiciones se incorporan pendientes de validación, sin niveles de apilamiento.</p>
              </section>
            </div>
          </div>
        )}
        <DialogFooter className="shrink-0 border-t bg-white px-5 py-3">
          <Button type="button" variant="outline" onClick={() => close(false)}>Cancelar</Button>
          <Button type="button" disabled={!canConfirm} onClick={() => { if (preview && canConfirm) onConfirm(preview) }}>Incorporar {preview ? preview.position_count + " posiciones" : "al borrador"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
