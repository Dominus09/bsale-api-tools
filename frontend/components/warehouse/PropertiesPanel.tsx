"use client"

import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import type { WarehouseElement, WarehouseElementType } from "@/lib/warehouse/types"
import { ELEMENT_TYPE_LABELS_ES } from "@/lib/warehouse/style"

type Props = {
  element: WarehouseElement | null
  parents: WarehouseElement[]
  onChange: (patch: {
    physical?: Partial<WarehouseElement["physical"]>
    geometry?: Partial<WarehouseElement["geometry"]>
  }) => void
  readOnly?: boolean
}

const TYPES = Object.keys(ELEMENT_TYPE_LABELS_ES) as WarehouseElementType[]

function NumField({
  label,
  value,
  onChange,
  disabled,
  allowEmpty,
}: {
  label: string
  value: number | null | undefined
  onChange: (v: number | null) => void
  disabled?: boolean
  allowEmpty?: boolean
}) {
  return (
    <div className="space-y-1">
      <Label className="text-xs text-slate-600">{label}</Label>
      <Input
        type="number"
        step="0.01"
        disabled={disabled}
        value={value == null ? "" : value}
        onChange={(e) => {
          const raw = e.target.value
          if (allowEmpty && raw === "") {
            onChange(null)
            return
          }
          const n = Number(raw)
          if (Number.isFinite(n)) onChange(n)
        }}
        className="h-8"
      />
    </div>
  )
}

export function PropertiesPanel({ element, parents, onChange, readOnly }: Props) {
  if (!element) {
    return (
      <div className="flex h-full items-center justify-center p-4 text-center text-sm text-slate-500">
        Seleccione un elemento del plano para editar sus propiedades.
      </div>
    )
  }

  const p = element.physical
  const g = element.geometry
  const disabled = Boolean(readOnly)

  return (
    <div className="flex h-full flex-col gap-3 overflow-y-auto p-3">
      <div>
        <h3 className="text-sm font-semibold text-slate-900">Propiedades</h3>
        <p className="text-xs text-slate-500">
          Cambios locales hasta «Guardar borrador». Identidad física id=
          {p.id > 0 ? p.id : "nuevo"}
        </p>
      </div>

      <div className="space-y-1">
        <Label className="text-xs">Nombre</Label>
        <Input
          disabled={disabled}
          value={p.name}
          onChange={(e) => onChange({ physical: { name: e.target.value } })}
          className="h-8"
        />
      </div>
      <div className="space-y-1">
        <Label className="text-xs">Código visible</Label>
        <Input
          disabled={disabled}
          value={p.code}
          onChange={(e) => onChange({ physical: { code: e.target.value } })}
          className="h-8"
        />
      </div>
      <div className="space-y-1">
        <Label className="text-xs">Tipo</Label>
        <Select
          disabled={disabled}
          value={p.element_type}
          onValueChange={(v) =>
            onChange({ physical: { element_type: v as WarehouseElementType } })
          }
        >
          <SelectTrigger className="h-8">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {TYPES.map((t) => (
              <SelectItem key={t} value={t}>
                {ELEMENT_TYPE_LABELS_ES[t]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1">
        <Label className="text-xs">Sector / padre</Label>
        <Select
          disabled={disabled}
          value={p.parent_id != null ? String(p.parent_id) : "none"}
          onValueChange={(v) =>
            onChange({
              physical: { parent_id: v === "none" ? null : Number(v) },
            })
          }
        >
          <SelectTrigger className="h-8">
            <SelectValue placeholder="Sin padre" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="none">Sin padre</SelectItem>
            {parents
              .filter((x) => x.physical.id !== p.id && x.physical.id > 0)
              .map((x) => (
                <SelectItem key={x.clientKey} value={String(x.physical.id)}>
                  {x.physical.code} — {x.physical.name}
                </SelectItem>
              ))}
          </SelectContent>
        </Select>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <NumField
          label="X (m)"
          value={g.x_m}
          disabled={disabled}
          onChange={(v) => onChange({ geometry: { x_m: v ?? 0 } })}
        />
        <NumField
          label="Y (m)"
          value={g.y_m}
          disabled={disabled}
          onChange={(v) => onChange({ geometry: { y_m: v ?? 0 } })}
        />
        <NumField
          label="Ancho (m)"
          value={g.width_m}
          disabled={disabled}
          onChange={(v) => {
            if (v != null && v > 0) onChange({ geometry: { width_m: v } })
          }}
        />
        <NumField
          label="Profundidad (m)"
          value={g.depth_m}
          disabled={disabled}
          onChange={(v) => {
            if (v != null && v > 0) onChange({ geometry: { depth_m: v } })
          }}
        />
        <NumField
          label="Rotación (°)"
          value={g.rotation_deg}
          disabled={disabled}
          onChange={(v) => onChange({ geometry: { rotation_deg: v ?? 0 } })}
        />
        <NumField
          label="Altura libre (m)"
          value={g.height_clear_m}
          disabled={disabled}
          allowEmpty
          onChange={(v) => onChange({ geometry: { height_clear_m: v } })}
        />
        <NumField
          label="Elevación (m)"
          value={g.elevation_m}
          disabled={disabled}
          onChange={(v) => onChange({ geometry: { elevation_m: v ?? 0 } })}
        />
        <NumField
          label="Niveles apil. (futuro)"
          value={g.stack_levels_max}
          disabled={disabled}
          allowEmpty
          onChange={(v) => onChange({ geometry: { stack_levels_max: v } })}
        />
      </div>

      <div className="space-y-1">
        <Label className="text-xs">Estado de validación</Label>
        <Select
          disabled={disabled}
          value={g.validation_status}
          onValueChange={(v) =>
            onChange({
              geometry: {
                validation_status: v as WarehouseElement["geometry"]["validation_status"],
              },
            })
          }
        >
          <SelectTrigger className="h-8">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="pending_validation">Pendiente de validación</SelectItem>
            <SelectItem value="draft">Borrador</SelectItem>
            <SelectItem value="validated">Validado</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="space-y-1">
        <Label className="text-xs">Restricciones (JSON)</Label>
        <Textarea
          disabled={disabled}
          className="min-h-[72px] font-mono text-xs"
          value={JSON.stringify(g.constraints_json || {}, null, 2)}
          onChange={(e) => {
            try {
              const parsed = JSON.parse(e.target.value || "{}")
              if (parsed && typeof parsed === "object") {
                onChange({ geometry: { constraints_json: parsed } })
              }
            } catch {
              /* ignore invalid json while typing */
            }
          }}
        />
      </div>

      <div className="space-y-1">
        <Label className="text-xs">Observaciones</Label>
        <Textarea
          disabled={disabled}
          className="min-h-[80px]"
          value={g.observations || ""}
          onChange={(e) => onChange({ geometry: { observations: e.target.value } })}
        />
      </div>
    </div>
  )
}
