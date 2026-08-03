"use client"

import { ChevronDown, RefreshCw, RotateCcw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { FILTER_WARNING_OPTIONS } from "@/lib/costos-v2/labels"

const ALL = "__all__"
export type CostV2CompanyFilterDraft = {
  dateFrom: string; dateTo: string; search: string; minChangePercent: string
  movement: string; situation: string; warning: string; onlyRelevantChanges: boolean
}

export function CostV2CompanyFilters({ draft, onChange, onApply, onClear, loading, disabled, moreOpen, onMoreOpenChange }: {
  draft: CostV2CompanyFilterDraft; onChange: (patch: Partial<CostV2CompanyFilterDraft>) => void
  onApply: () => void; onClear: () => void; loading?: boolean; disabled?: boolean
  moreOpen: boolean; onMoreOpenChange: (open: boolean) => void
}) {
  return <div className="space-y-2 rounded-md border border-border/70 bg-card/30 p-3">
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
      <Field label="Fecha desde"><Input type="date" value={draft.dateFrom} onChange={(e) => onChange({ dateFrom: e.target.value })} disabled={disabled} /></Field>
      <Field label="Fecha hasta"><Input type="date" value={draft.dateTo} onChange={(e) => onChange({ dateTo: e.target.value })} disabled={disabled} /></Field>
      <Field label="Buscador"><Input value={draft.search} placeholder="Producto / código" onChange={(e) => onChange({ search: e.target.value })} disabled={disabled} /></Field>
      <div className="flex items-end gap-2 lg:col-span-2">
        <Button type="button" className="flex-1 bg-red-700 hover:bg-red-800" size="sm" onClick={onApply} disabled={disabled || loading}><RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />Actualizar</Button>
        <Button type="button" className="flex-1" variant="outline" size="sm" onClick={onClear} disabled={disabled || loading}><RotateCcw className="mr-1.5 h-3.5 w-3.5" />Limpiar</Button>
      </div>
    </div>
    <Collapsible open={moreOpen} onOpenChange={onMoreOpenChange}>
      <CollapsibleTrigger asChild><Button type="button" variant="ghost" size="sm" className="h-7 px-2 text-xs"><ChevronDown className="mr-1 h-3.5 w-3.5" />Más filtros</Button></CollapsibleTrigger>
      <CollapsibleContent className="mt-2 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
        <Field label="Cambio mínimo %"><Input inputMode="decimal" placeholder="10" value={draft.minChangePercent} onChange={(e) => onChange({ minChangePercent: e.target.value })} /></Field>
        <SelectField label="Movimiento" value={draft.movement} onChange={(movement) => onChange({ movement })} options={[["", "Todos"], ["up", "Alzas"], ["down", "Bajas"], ["flat", "Sin cambio"]]} />
        <SelectField label="Situación" value={draft.situation} onChange={(situation) => onChange({ situation })} options={[["", "Todos"], ["requires_review", "Requiere revisión"], ["office_difference", "Diferencia entre oficinas"], ["partial_coverage", "Cobertura parcial"]]} />
        <SelectField label="Alerta" value={draft.warning} onChange={(warning) => onChange({ warning })} options={[["", "Todas"], ...FILTER_WARNING_OPTIONS.map((x) => [x.value, x.label])]} />
        <label className="flex items-center gap-2 pt-6 text-sm"><input type="checkbox" checked={draft.onlyRelevantChanges} onChange={(e) => onChange({ onlyRelevantChanges: e.target.checked })} />Solo cambios relevantes</label>
      </CollapsibleContent>
    </Collapsible>
  </div>
}
function Field({ label, children }: { label: string; children: React.ReactNode }) { return <div className="min-w-0 space-y-1"><Label>{label}</Label>{children}</div> }
function SelectField({ label, value, onChange, options }: { label: string; value: string; onChange: (v: string) => void; options: string[][] }) {
  return <Field label={label}><Select value={value || ALL} onValueChange={(v) => onChange(v === ALL ? "" : v)}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent>{options.map(([v, text]) => <SelectItem key={v || ALL} value={v || ALL}>{text}</SelectItem>)}</SelectContent></Select></Field>
}
