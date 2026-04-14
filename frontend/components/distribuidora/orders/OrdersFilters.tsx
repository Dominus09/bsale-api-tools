"use client"

import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { cn } from "@/lib/utils"

export type SellerOption = { user_id: number; label: string }

export type MunicipalityOption = { value: string; label: string }

export const WEEKDAY_DELIVERY_OPTIONS = [
  "Lunes",
  "Martes",
  "Miércoles",
  "Jueves",
  "Viernes",
  "Sábado",
  "Domingo",
] as const

type OrdersFiltersProps = {
  dateFrom: string
  onDateFromChange: (isoDate: string) => void
  dateTo: string
  onDateToChange: (isoDate: string) => void
  selectedDeliveryDays: ReadonlySet<string>
  onToggleDeliveryDay: (day: string) => void
  onClearDeliveryDays: () => void
  deliveryExtraText: string
  onDeliveryExtraTextChange: (value: string) => void
  sellerOptions: SellerOption[]
  sellerUserId: string
  onSellerUserIdChange: (value: string) => void
  municipalityOptions: MunicipalityOption[]
  selectedMunicipalityKeys: ReadonlySet<string>
  onToggleMunicipality: (value: string) => void
  onClearMunicipalities: () => void
  onlyNotInvoiced: boolean
  onOnlyNotInvoicedChange: (value: boolean) => void
  loading?: boolean
}

export function OrdersFilters({
  dateFrom,
  onDateFromChange,
  dateTo,
  onDateToChange,
  selectedDeliveryDays,
  onToggleDeliveryDay,
  onClearDeliveryDays,
  deliveryExtraText,
  onDeliveryExtraTextChange,
  sellerOptions,
  sellerUserId,
  onSellerUserIdChange,
  municipalityOptions,
  selectedMunicipalityKeys,
  onToggleMunicipality,
  onClearMunicipalities,
  onlyNotInvoiced,
  onOnlyNotInvoicedChange,
  loading,
}: OrdersFiltersProps) {
  const allMunicipalities =
    selectedMunicipalityKeys.size === 0 ? "Todas las comunas" : `${selectedMunicipalityKeys.size} comuna(s)`

  return (
    <div className="flex flex-col gap-6 rounded-lg border bg-card p-4 shadow-sm">
      <h2 className="text-sm font-semibold text-foreground">Filtros</h2>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="space-y-2">
          <Label htmlFor="orders-date-from">Fecha desde</Label>
          <Input
            id="orders-date-from"
            type="date"
            value={dateFrom}
            onChange={(e) => onDateFromChange(e.target.value)}
            disabled={loading}
          />
          <p className="text-xs text-muted-foreground">
            Mismo día en desde y hasta = un solo día.
          </p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="orders-date-to">Fecha hasta</Label>
          <Input
            id="orders-date-to"
            type="date"
            value={dateTo}
            onChange={(e) => onDateToChange(e.target.value)}
            disabled={loading}
          />
        </div>
        <div className="flex items-end pb-1 lg:col-span-2">
          <label className="flex cursor-pointer items-center gap-2 text-sm">
            <Checkbox
              checked={onlyNotInvoiced}
              onCheckedChange={(c) => onOnlyNotInvoicedChange(c === true)}
              disabled={loading}
            />
            Solo no facturadas
          </label>
        </div>
      </div>

      <div className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <Label className="text-sm font-medium">Día de entrega (observaciones)</Label>
          <div className="flex flex-wrap gap-2">
            {selectedDeliveryDays.size > 0 || deliveryExtraText.trim() ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-8 text-xs"
                onClick={() => {
                  onClearDeliveryDays()
                  onDeliveryExtraTextChange("")
                }}
                disabled={loading}
              >
                Limpiar días
              </Button>
            ) : null}
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Varios días = OR en observaciones. Sin selección = sin filtro por día.
        </p>
        <div className="flex flex-wrap gap-2">
          {WEEKDAY_DELIVERY_OPTIONS.map((d) => {
            const on = selectedDeliveryDays.has(d)
            return (
              <button
                key={d}
                type="button"
                onClick={() => onToggleDeliveryDay(d)}
                disabled={loading}
                className={cn(
                  "rounded-full border px-3 py-1.5 text-sm font-medium transition-colors",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                  on
                    ? "border-primary bg-primary text-primary-foreground shadow-sm"
                    : "border-border bg-background text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                  loading && "pointer-events-none opacity-60",
                )}
              >
                {d}
              </button>
            )
          })}
        </div>
        <div className="space-y-2">
          <Label htmlFor="orders-delivery-extra" className="text-xs text-muted-foreground">
            Texto adicional en observaciones (opcional)
          </Label>
          <Input
            id="orders-delivery-extra"
            type="text"
            placeholder="Ej. reparto, ruta norte…"
            value={deliveryExtraText}
            onChange={(e) => onDeliveryExtraTextChange(e.target.value)}
            disabled={loading}
          />
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label className="text-sm font-medium">Ciudad / comuna</Label>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 text-xs"
              onClick={onClearMunicipalities}
              disabled={loading || selectedMunicipalityKeys.size === 0}
            >
              Todas las comunas
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            {allMunicipalities}. Varias comunas = OR. Lista según resultados del rango
            (ajuste fechas para ver más).
          </p>
          <div className="max-h-44 overflow-y-auto rounded-md border bg-muted/20 p-3 text-sm">
            {municipalityOptions.length === 0 ? (
              <span className="text-muted-foreground">Sin comunas en el resultado actual.</span>
            ) : (
              <ul className="space-y-2.5">
                {municipalityOptions.map((m) => (
                  <li key={m.value} className="flex items-center gap-2.5">
                    <Checkbox
                      id={`ord-muni-${encodeURIComponent(m.value)}`}
                      checked={selectedMunicipalityKeys.has(m.value)}
                      onCheckedChange={() => onToggleMunicipality(m.value)}
                      disabled={loading}
                    />
                    <label
                      htmlFor={`ord-muni-${encodeURIComponent(m.value)}`}
                      className="cursor-pointer leading-tight"
                    >
                      {m.label}
                    </label>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        <div className="space-y-2">
          <Label>Vendedor</Label>
          <Select
            value={sellerUserId === "" ? "__all_sellers__" : sellerUserId}
            onValueChange={(v) =>
              onSellerUserIdChange(v === "__all_sellers__" ? "" : v)
            }
            disabled={loading}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all_sellers__">Todos</SelectItem>
              {sellerOptions.map((o) => (
                <SelectItem key={o.user_id} value={String(o.user_id)}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  )
}
