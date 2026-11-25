"use client"

import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
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
  previewGeneratePositions,
  type GeneratePreviewResult,
} from "@/lib/warehouse/api"
import { GENERATOR_PRESETS, STANDARD_PALLET_M } from "@/lib/warehouse/style"
import type { WarehouseElement } from "@/lib/warehouse/types"

type Props = {
  open: boolean
  onOpenChange: (open: boolean) => void
  parent: WarehouseElement | null
  floorLengthM: number
  floorWidthM: number
  existing: WarehouseElement[]
  onConfirm: (preview: GeneratePreviewResult) => void
}

export function GeneratePositionsDialog({
  open,
  onOpenChange,
  parent,
  floorLengthM,
  floorWidthM,
  existing,
  onConfirm,
}: Props) {
  const [preset, setPreset] = useState<string>("4+5")
  const [rowCountsText, setRowCountsText] = useState("4,5")
  const [orientation, setOrientation] = useState("standard")
  const [rowOrientsText, setRowOrientsText] = useState("")
  const [palletL, setPalletL] = useState(STANDARD_PALLET_M.length)
  const [palletW, setPalletW] = useState(STANDARD_PALLET_M.width)
  const [gapLong, setGapLong] = useState(0)
  const [gapTrans, setGapTrans] = useState(0)
  const [marginStart, setMarginStart] = useState(0)
  const [marginEnd, setMarginEnd] = useState(0)
  const [marginNear, setMarginNear] = useState(0)
  const [marginFar, setMarginFar] = useState(0)
  const [anchor, setAnchor] = useState("end")
  const [growRows, setGrowRows] = useState("near_to_far")
  const [suffixMode, setSuffixMode] = useState("numeric")
  const [prefix, setPrefix] = useState("")
  const [startIndex, setStartIndex] = useState(1)
  const [heightClear, setHeightClear] = useState<string>("")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [preview, setPreview] = useState<GeneratePreviewResult | null>(null)

  useEffect(() => {
    if (!open || !parent) return
    setPrefix(parent.physical.code)
    setSuffixMode(parent.physical.code === "IZ" ? "iz_da" : "numeric")
    setHeightClear(
      parent.geometry.height_clear_m != null ? String(parent.geometry.height_clear_m) : "",
    )
    setPreview(null)
    setError(null)
  }, [open, parent])

  useEffect(() => {
    if (preset !== "custom" && preset.includes("+")) {
      setRowCountsText(preset.replace("+", ","))
    }
  }, [preset])

  const parentLabel = parent ? `${parent.physical.code} — ${parent.physical.name}` : "—"

  const canPreview = Boolean(parent)

  const runPreview = async () => {
    if (!parent) return
    setLoading(true)
    setError(null)
    try {
      const row_counts = rowCountsText
        .split(/[,+\s]+/)
        .map((s) => parseInt(s, 10))
        .filter((n) => Number.isFinite(n) && n > 0)
      const row_orientations = rowOrientsText
        .split(/[,\s]+/)
        .map((s) => s.trim())
        .filter(Boolean)
      const obstacles = existing
        .filter((e) =>
          ["obstacle", "pillar", "wall", "stair", "gate", "circulation", "unusable"].includes(
            e.physical.element_type,
          ),
        )
        .map((e) => ({
          physical: e.physical,
          geometry: e.geometry,
        }))
      const result = await previewGeneratePositions({
        parent: {
          x_m: parent.geometry.x_m,
          y_m: parent.geometry.y_m,
          width_m: parent.geometry.width_m,
          depth_m: parent.geometry.depth_m,
          rotation_deg: parent.geometry.rotation_deg,
          code: parent.physical.code,
          element_type: parent.physical.element_type,
        },
        parent_physical_id: parent.physical.id > 0 ? parent.physical.id : null,
        pallet_length_m: palletL,
        pallet_width_m: palletW,
        preset,
        row_counts,
        row_orientations,
        default_orientation: orientation,
        gap_longitudinal_m: gapLong,
        gap_transverse_m: gapTrans,
        margin_start_m: marginStart,
        margin_end_m: marginEnd,
        margin_near_m: marginNear,
        margin_far_m: marginFar,
        anchor,
        grow_rows: growRows,
        code_prefix: prefix || parent.physical.code,
        code_suffix_mode: suffixMode,
        start_index: startIndex,
        height_clear_m: heightClear === "" ? null : Number(heightClear),
        floor_length_m: floorLengthM,
        floor_width_m: floorWidthM,
        existing_elements: existing.map((e) => ({
          physical: e.physical,
          geometry: e.geometry,
        })),
        obstacles,
        allow_outside_parent: true,
      })
      setPreview(result)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error en vista previa")
      setPreview(null)
    } finally {
      setLoading(false)
    }
  }

  const summary = useMemo(() => {
    if (!preview) return null
    return (
      <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm">
        <div>
          <strong>{preview.position_count}</strong> posiciones · superficie{" "}
          <strong>{preview.occupied_area_m2.toFixed(2)} m²</strong>
        </div>
        <div>
          Separación interior filas:{" "}
          {preview.interior_gap_m != null ? `${preview.interior_gap_m.toFixed(2)} m` : "—"}
          {preview.remaining_transverse_m != null
            ? ` · holgura transversal ${preview.remaining_transverse_m.toFixed(2)} m`
            : ""}
        </div>
        <div>
          Extensión fuera del sector/pasillo:{" "}
          <strong>{preview.extension_outside_parent_m.toFixed(2)} m</strong>
          {preview.extension_outside_parent_m > 0
            ? " (no autoriza invasión de circulación)"
            : ""}
        </div>
        {(preview.obstacle_conflicts?.length || 0) > 0 ? (
          <div className="text-red-700">
            Conflictos con obstáculos/estructuras: {preview.obstacle_conflicts!.length}
          </div>
        ) : (
          <div className="text-emerald-700">Sin conflictos de obstáculo reportados en preview</div>
        )}
        {(preview.circulation_warnings?.length || 0) > 0 ? (
          <div className="text-amber-800">
            Advertencias de circulación: {preview.circulation_warnings!.length}
          </div>
        ) : null}
        {preview.warnings?.map((w, i) => (
          <div key={i} className="text-amber-800">
            {w.message}
          </div>
        ))}
        <p className="text-xs text-slate-500">{preview.note_es}</p>
        <div className="max-h-28 overflow-y-auto font-mono text-[11px] text-slate-600">
          {preview.positions.map((p) => (
            <div key={p.code}>
              {p.code}: {Number(p.geometry.x_m).toFixed(2)},{Number(p.geometry.y_m).toFixed(2)} ·{" "}
              {Number(p.geometry.width_m).toFixed(2)}×{Number(p.geometry.depth_m).toFixed(2)}
            </div>
          ))}
        </div>
      </div>
    )
  }, [preview])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] max-w-3xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Generar posiciones</DialogTitle>
        </DialogHeader>
        {!parent ? (
          <p className="text-sm text-amber-800">
            Seleccione un sector o pasillo en el plano antes de abrir el generador.
          </p>
        ) : (
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-3">
              <p className="text-sm text-slate-600">
                Padre: <strong>{parentLabel}</strong> ({parent.geometry.width_m.toFixed(2)}×
                {parent.geometry.depth_m.toFixed(2)} m)
              </p>
              <div className="space-y-1">
                <Label className="text-xs">Preset</Label>
                <Select value={preset} onValueChange={setPreset}>
                  <SelectTrigger className="h-8">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {GENERATOR_PRESETS.map((p) => (
                      <SelectItem key={p} value={p}>
                        {p}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Filas (conteos, ej. 4,5)</Label>
                <Input
                  className="h-8"
                  value={rowCountsText}
                  onChange={(e) => {
                    setPreset("custom")
                    setRowCountsText(e.target.value)
                  }}
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Pallet largo (m)</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={palletL}
                    onChange={(e) => setPalletL(Number(e.target.value))}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Pallet ancho (m)</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={palletW}
                    onChange={(e) => setPalletW(Number(e.target.value))}
                  />
                </div>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Orientación por defecto</Label>
                <Select value={orientation} onValueChange={setOrientation}>
                  <SelectTrigger className="h-8">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="standard">
                      Estándar (1,26 en profundidad del pasillo)
                    </SelectItem>
                    <SelectItem value="rotated">
                      Girada (1,06 en profundidad del pasillo)
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">
                  Orientaciones por fila (opcional, ej. standard,rotated)
                </Label>
                <Input
                  className="h-8"
                  value={rowOrientsText}
                  onChange={(e) => setRowOrientsText(e.target.value)}
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Gap longitudinal</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={gapLong}
                    onChange={(e) => setGapLong(Number(e.target.value))}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Gap transversal</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={gapTrans}
                    onChange={(e) => setGapTrans(Number(e.target.value))}
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Margen inicio Y</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={marginStart}
                    onChange={(e) => setMarginStart(Number(e.target.value))}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Margen fondo Y</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={marginEnd}
                    onChange={(e) => setMarginEnd(Number(e.target.value))}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Margen near X</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={marginNear}
                    onChange={(e) => setMarginNear(Number(e.target.value))}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Margen far X</Label>
                  <Input
                    type="number"
                    step="0.01"
                    className="h-8"
                    value={marginFar}
                    onChange={(e) => setMarginFar(Number(e.target.value))}
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Anclaje</Label>
                  <Select value={anchor} onValueChange={setAnchor}>
                    <SelectTrigger className="h-8">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="start">Inicio (Y bajo)</SelectItem>
                      <SelectItem value="end">Fondo (Y alto)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Crecimiento filas</Label>
                  <Select value={growRows} onValueChange={setGrowRows}>
                    <SelectTrigger className="h-8">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="near_to_far">Near → far</SelectItem>
                      <SelectItem value="far_to_near">Far → near</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-xs">Prefijo código</Label>
                  <Input className="h-8" value={prefix} onChange={(e) => setPrefix(e.target.value)} />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Inicio numeración</Label>
                  <Input
                    type="number"
                    className="h-8"
                    value={startIndex}
                    onChange={(e) => setStartIndex(Number(e.target.value))}
                  />
                </div>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Sufijo</Label>
                <Select value={suffixMode} onValueChange={setSuffixMode}>
                  <SelectTrigger className="h-8">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="numeric">Numérico (P2-01)</SelectItem>
                    <SelectItem value="iz_da">IZ D/A (IZ-01-D)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">Altura libre (m, opcional — no autoriza apilamiento)</Label>
                <Input
                  className="h-8"
                  value={heightClear}
                  onChange={(e) => setHeightClear(e.target.value)}
                  placeholder="vacío = desconocido"
                />
              </div>
            </div>
            <div className="space-y-3">
              <Button type="button" disabled={!canPreview || loading} onClick={() => void runPreview()}>
                {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Vista previa (no guarda)
              </Button>
              {error ? <p className="text-sm text-red-700">{error}</p> : null}
              {summary}
            </div>
          </div>
        )}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancelar
          </Button>
          <Button
            type="button"
            disabled={!preview || preview.position_count === 0}
            onClick={() => {
              if (preview) onConfirm(preview)
            }}
          >
            Incorporar al borrador
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
